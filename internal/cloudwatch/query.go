package cloudwatch

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/mtanda/cloudwatch_metrics_proxy/internal/index"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	prom_value "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

const (
	PROMETHEUS_MAXIMUM_POINTS = 11000
	indexInterval             = time.Duration(60) * time.Minute // TODO: get from labels database
)

var (
	cloudwatchApiCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cloudwatch_metrics_proxy_cloudwatch_api_calls_total",
			Help: "The total number of CloudWatch API calls",
		},
		[]string{"api", "namespace", "from", "status"},
	)
	clientCache = make(map[string]*cloudwatch.Client)
)

func init() {
	prometheus.MustRegister(cloudwatchApiCalls)
}

type CloudWatchClient struct {
	labelDBUrl    string
	maximumStep   int64
	lookbackDelta time.Duration
	readHints     prompb.ReadHints
}

func New(labelDBUrl string, maximumStep int64, lookbackDelta time.Duration, readHints prompb.ReadHints) (*CloudWatchClient, error) {
	return &CloudWatchClient{
		labelDBUrl:    labelDBUrl,
		maximumStep:   maximumStep,
		lookbackDelta: lookbackDelta,
		readHints:     readHints,
	}, nil

}

func (c *CloudWatchClient) GetQuery(ctx context.Context, q *prompb.Query) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	// index doesn't have statistics label, get label matchers without statistics
	mm := make([]*prompb.LabelMatcher, 0)
	ms := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Name == "Statistic" || m.Name == "ExtendedStatistic" || m.Name == "Period" {
			ms = append(ms, m)
		} else {
			mm = append(mm, m)
		}
	}

	matchers, err := parseQueryMatchers(mm)
	if err != nil {
		return "", nil, err
	}

	stat, eStat, period, err := parseQueryParams(ms, c.maximumStep)
	if err != nil {
		return "", nil, err
	}

	region, queries, err := c.getQueryWithIndex(ctx, matchers, stat, eStat, period)
	if err != nil {
		return "", nil, err
	}

	// if no queries are generated, try to get time series without index
	if len(queries) == 0 {
		region, queries, err = c.getQueryWithoutIndex(matchers, stat, eStat, period)
		if err != nil {
			return "", nil, err
		}
	}

	return region, queries, nil
}

func (c *CloudWatchClient) getQueryWithoutIndex(matchers []*labels.Matcher, stat []types.Statistic, eStat []string, period int32) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	query := &cloudwatch.GetMetricStatisticsInput{}
	for _, m := range matchers {
		if m.Type != labels.MatchEqual {
			continue // only support equal matcher
		}

		oldMetricName := ""
		switch m.Name {
		case "__name__":
			oldMetricName = m.Value
		case "Region":
			region = m.Value
		case "Namespace":
			query.Namespace = aws.String(m.Value)
		case "MetricName":
			query.MetricName = aws.String(m.Value)
		default:
			if m.Value != "" {
				query.Dimensions = append(query.Dimensions, types.Dimension{
					Name:  aws.String(m.Name),
					Value: aws.String(m.Value),
				})
			}
		}
		if query.MetricName == nil {
			query.MetricName = aws.String(oldMetricName) // backward compatibility
		}
	}
	if len(stat) > 0 {
		query.Statistics = stat
	}
	if len(eStat) > 0 {
		query.ExtendedStatistics = eStat
	}
	if period != 0 {
		query.Period = aws.Int32(period)
	}
	query.StartTime = aws.Time(time.Unix(int64(c.readHints.StartMs/1000), int64(c.readHints.StartMs%1000*1000)))
	query.EndTime = aws.Time(time.Unix(int64(c.readHints.EndMs/1000), int64(c.readHints.EndMs%1000*1000)))
	queries = append(queries, query)

	if region == "" {
		region, err := getDefaultRegion()
		if err != nil {
			return region, queries, err
		}
	}

	return region, queries, nil
}

func (c *CloudWatchClient) getQueryWithIndex(ctx context.Context, matchers []*labels.Matcher, stat []types.Statistic, eStat []string, period int32) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	matchLabelsStartMs := c.readHints.StartMs
	if time.Unix(c.readHints.EndMs/1000, 0).Sub(time.Unix(c.readHints.StartMs/1000, 0)) < 2*indexInterval {
		// expand enough long period to match index
		matchLabelsStartMs = time.Unix(c.readHints.EndMs/1000, 0).Add(-2*indexInterval).Unix() * 1000
	}
	matchedLabelsList, err := index.GetMatchedLabels(ctx, c.labelDBUrl, matchers, matchLabelsStartMs/1000, c.readHints.EndMs/1000)
	if err != nil {
		return region, queries, err
	}

	for _, matchedLabels := range matchedLabelsList {
		query := &cloudwatch.GetMetricStatisticsInput{}
		for _, label := range matchedLabels {
			oldMetricName := ""
			switch label.Name {
			case "__name__":
				oldMetricName = label.Value
			case "Region":
				// TODO: support multiple region?
				region = label.Value
			case "Namespace":
				query.Namespace = aws.String(label.Value)
			case "MetricName":
				query.MetricName = aws.String(label.Value)
			default:
				if query.Dimensions == nil {
					query.Dimensions = make([]types.Dimension, 0)
				}
				if label.Value != "" {
					query.Dimensions = append(query.Dimensions, types.Dimension{
						Name:  aws.String(label.Name),
						Value: aws.String(label.Value),
					})
				}
			}
			if query.MetricName == nil {
				query.MetricName = aws.String(oldMetricName) // backward compatibility
			}
		}
		if len(stat) > 0 {
			query.Statistics = stat
		}
		if len(eStat) > 0 {
			query.ExtendedStatistics = eStat
		}
		if period != 0 {
			query.Period = aws.Int32(period)
		}
		// set default statistics
		if len(query.Statistics) == 0 && len(query.ExtendedStatistics) == 0 {
			query.Statistics = []types.Statistic{types.Statistic("Sum"), types.Statistic("SampleCount"), types.Statistic("Maximum"), types.Statistic("Minimum"), types.Statistic("Average")}
			query.ExtendedStatistics = []string{"p50.00", "p90.00", "p95.00", "p99.00"}
		}
		query.StartTime = aws.Time(time.Unix(int64(c.readHints.StartMs/1000), int64(c.readHints.StartMs%1000*1000)))
		query.EndTime = aws.Time(time.Unix(int64(c.readHints.EndMs/1000), int64(c.readHints.EndMs%1000*1000)))
		queries = append(queries, query)
	}

	return region, queries, nil
}

func (c *CloudWatchClient) QueryCloudWatch(ctx context.Context, region string, queries []*cloudwatch.GetMetricStatisticsInput) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries

	// switch to GetMetricData if the query is not single statistic
	if !isSingleStatistic(queries) {
		if len(queries) > 200 {
			return result, fmt.Errorf("Too many concurrent queries")
		}
		for _, query := range queries {
			cwResult, err := c.queryCloudWatchGetMetricStatistics(ctx, region, query)
			if err != nil {
				return result, err
			}
			result = append(result, cwResult...)
		}
	} else {
		if len(queries)/70 > 25 {
			return result, fmt.Errorf("Too many concurrent queries")
		}
		for i := 0; i < len(queries); i += 70 {
			e := int(math.Min(float64(i+70), float64(len(queries))))
			cwResult, err := c.queryCloudWatchGetMetricData(ctx, region, queries[i:e])
			if err != nil {
				return result, err
			}
			result = append(result, cwResult...)
		}
	}
	return result, nil
}

func (c *CloudWatchClient) queryCloudWatchGetMetricStatistics(ctx context.Context, region string, query *cloudwatch.GetMetricStatisticsInput) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries
	svc, err := getClient(ctx, region)
	if err != nil {
		return nil, err
	}

	if query.Namespace == nil || query.MetricName == nil {
		return result, fmt.Errorf("missing parameter")
	}

	// align time range
	periodUnit := calibratePeriod(*query.StartTime)
	rangeAdjust := 0 * time.Second
	if c.readHints.StartMs%int64(periodUnit*1000) != 0 {
		rangeAdjust = time.Duration(periodUnit) * time.Second
	}
	query.StartTime = aws.Time(query.StartTime.Truncate(time.Duration(periodUnit)))
	if (*query.StartTime).Add(rangeAdjust).Before(*query.EndTime) {
		query.StartTime = aws.Time(query.StartTime.Add(rangeAdjust))
	}
	query.EndTime = aws.Time(query.EndTime.Truncate(time.Duration(periodUnit)))

	// auto calibrate period
	highResolution := true
	if query.Period == nil {
		query.Period = calcQueryPeriod(*query.StartTime, *query.EndTime, periodUnit)
		highResolution = false
	}

	startTime := *query.StartTime
	endTime := *query.EndTime
	var resp *cloudwatch.GetMetricStatisticsOutput
	for startTime.Before(endTime) {
		query.StartTime = aws.Time(startTime)
		if highResolution {
			startTime = startTime.Add(time.Duration(1440*(*query.Period)) * time.Second)
		} else {
			startTime = endTime
		}
		query.EndTime = aws.Time(startTime)

		partResp, err := svc.GetMetricStatistics(ctx, query)
		if err != nil {
			cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *query.Namespace, "query", "error").Add(float64(1))
			return result, err
		}
		if resp != nil {
			resp.Datapoints = append(resp.Datapoints, partResp.Datapoints...)
		} else {
			resp = partResp
		}
		cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *query.Namespace, "query", "success").Add(float64(1))
		if len(resp.Datapoints) > PROMETHEUS_MAXIMUM_POINTS {
			return result, fmt.Errorf("exceed maximum datapoints")
		}
	}

	// make time series
	statistics := make([]string, 0)
	for _, s := range query.Statistics {
		statistics = append(statistics, string(s))
	}
	paramStatistics := append(statistics, query.ExtendedStatistics...)
	tsm := make(map[string]*prompb.TimeSeries)
	for _, s := range paramStatistics {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, prompb.Label{Name: "Region", Value: region})
		ts.Labels = append(ts.Labels, prompb.Label{Name: "Namespace", Value: *query.Namespace})
		ts.Labels = append(ts.Labels, prompb.Label{Name: "__name__", Value: safeMetricName(*query.MetricName)})
		for _, d := range query.Dimensions {
			ts.Labels = append(ts.Labels, prompb.Label{Name: *d.Name, Value: *d.Value})
		}
		if !isExtendedStatistics(s) {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "Statistic", Value: s})
		} else {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "ExtendedStatistic", Value: s})
		}
		tsm[s] = ts
	}

	sort.Slice(resp.Datapoints, func(i, j int) bool {
		return resp.Datapoints[i].Timestamp.Before(*resp.Datapoints[j].Timestamp)
	})
	var lastTimestamp time.Time
	for _, dp := range resp.Datapoints {
		for _, s := range paramStatistics {
			value := 0.0
			if !isExtendedStatistics(s) {
				switch s {
				case "Sum":
					value = *dp.Sum
				case "SampleCount":
					value = *dp.SampleCount
				case "Maximum":
					value = *dp.Maximum
				case "Minimum":
					value = *dp.Minimum
				case "Average":
					value = *dp.Average
				}
			} else {
				if dp.ExtendedStatistics == nil {
					continue
				}
				value = dp.ExtendedStatistics[s]
			}
			ts := tsm[s]
			if *query.Period > 60 && !lastTimestamp.IsZero() && lastTimestamp.Add(time.Duration(*query.Period)*time.Second).Before(*dp.Timestamp) {
				// set stale NaN at gap
				ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + int64(*query.Period)) * 1000})
			}
			ts.Samples = append(ts.Samples, prompb.Sample{Value: value, Timestamp: dp.Timestamp.Unix() * 1000})
		}
		lastTimestamp = *dp.Timestamp
	}

	// set stale NaN at the end
	if *query.Period > 60 && !lastTimestamp.IsZero() && lastTimestamp.Before(endTime) && lastTimestamp.Before(time.Now().UTC().Add(-c.lookbackDelta)) {
		for _, s := range paramStatistics {
			ts := tsm[s]
			ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + int64(*query.Period)) * 1000})
		}
	}

	for _, ts := range tsm {
		result = append(result, ts)
	}

	return result, nil
}

func (c *CloudWatchClient) queryCloudWatchGetMetricData(ctx context.Context, region string, queries []*cloudwatch.GetMetricStatisticsInput) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries
	svc, err := getClient(ctx, region)
	if err != nil {
		return nil, err
	}

	params := &cloudwatch.GetMetricDataInput{
		ScanBy: types.ScanBy("TimestampAscending"),
	}
	var namespace string
	var period int32
	periodUnit := calibratePeriod(*queries[0].StartTime)

	// convert to GetMetricData query
	for i, query := range queries {
		// auto calibrate period
		if query.Period == nil {
			query.Period = calcQueryPeriod(*query.StartTime, *query.EndTime, periodUnit)
		}

		mdq := types.MetricDataQuery{
			Id:         aws.String("id" + strconv.Itoa(i)),
			ReturnData: aws.Bool(true),
		}
		mdq.MetricStat = &types.MetricStat{
			Metric: &types.Metric{
				Namespace:  query.Namespace,
				MetricName: query.MetricName,
			},
			Period: query.Period,
		}
		namespace = *query.Namespace
		period = *query.Period
		for _, d := range query.Dimensions {
			mdq.MetricStat.Metric.Dimensions = append(mdq.MetricStat.Metric.Dimensions,
				types.Dimension{
					Name:  d.Name,
					Value: d.Value,
				})
		}
		if len(query.Statistics) == 1 {
			mdq.MetricStat.Stat = aws.String(string(query.Statistics[0]))
		} else if len(query.ExtendedStatistics) == 1 {
			mdq.MetricStat.Stat = aws.String(query.ExtendedStatistics[0])
		} else {
			return result, fmt.Errorf("no statistics specified")
		}
		params.MetricDataQueries = append(params.MetricDataQueries, mdq)

		// query range should be same among queries
		params.StartTime = query.StartTime
		params.EndTime = query.EndTime
	}

	// align time range
	rangeAdjust := 0 * time.Second
	if c.readHints.StartMs%int64(periodUnit*1000) != 0 {
		rangeAdjust = time.Duration(periodUnit) * time.Second
	}
	params.StartTime = aws.Time(params.StartTime.Truncate(time.Duration(periodUnit)))
	if (*params.StartTime).Add(rangeAdjust).Before(*params.EndTime) {
		params.StartTime = aws.Time(params.StartTime.Add(rangeAdjust))
	}
	params.EndTime = aws.Time(params.EndTime.Truncate(time.Duration(periodUnit)))
	if (params.EndTime).Sub(*params.StartTime)/(time.Duration(period)*time.Second) > PROMETHEUS_MAXIMUM_POINTS {
		return result, fmt.Errorf("exceed maximum datapoints")
	}

	// set labels to result
	tsm := make(map[string]*prompb.TimeSeries)
	for _, r := range params.MetricDataQueries {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, prompb.Label{Name: "Region", Value: region})
		ts.Labels = append(ts.Labels, prompb.Label{Name: "Namespace", Value: *r.MetricStat.Metric.Namespace})
		ts.Labels = append(ts.Labels, prompb.Label{Name: "__name__", Value: safeMetricName(*r.MetricStat.Metric.MetricName)})
		for _, d := range r.MetricStat.Metric.Dimensions {
			ts.Labels = append(ts.Labels, prompb.Label{Name: *d.Name, Value: *d.Value})
		}
		s := *r.MetricStat.Stat
		if !isExtendedStatistics(s) {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "Statistic", Value: s})
		} else {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "ExtendedStatistic", Value: s})
		}
		tsm[*r.Id] = ts
	}

	// get metrics data and set it to result
	nextToken := ""
	for {
		if nextToken != "" {
			params.NextToken = aws.String(nextToken)
		}

		resp, err := svc.GetMetricData(ctx, params)
		if err != nil {
			cloudwatchApiCalls.WithLabelValues("GetMetricData", namespace, "query", "error").Add(float64(len(params.MetricDataQueries)))
			return nil, err
		}
		cloudwatchApiCalls.WithLabelValues("GetMetricData", namespace, "query", "success").Add(float64(len(params.MetricDataQueries)))
		for _, r := range resp.MetricDataResults {
			ts := tsm[*r.Id]
			for i, t := range r.Timestamps {
				ts.Samples = append(ts.Samples, prompb.Sample{Value: r.Values[i], Timestamp: t.Unix() * 1000})
				if period <= 60 {
					continue
				}

				if i != len(r.Timestamps) && i != 0 {
					// set stale NaN at gap
					lastTimestamp := r.Timestamps[i-1]
					if lastTimestamp.Add(time.Duration(period) * time.Second).Before(r.Timestamps[i]) {
						ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + int64(period)) * 1000})
					}
				} else if i == len(r.Timestamps) {
					// set stale NaN at the end
					lastTimestamp := r.Timestamps[i]
					if lastTimestamp.Before(*params.EndTime) && lastTimestamp.Before(time.Now().UTC().Add(-c.lookbackDelta)) {
						ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + int64(period)) * 1000})
					}
				}
			}
		}

		if resp.NextToken == nil || *resp.NextToken == "" {
			break
		}
		nextToken = *resp.NextToken
	}

	for _, ts := range tsm {
		result = append(result, ts)
	}

	return result, nil
}

func (c *CloudWatchClient) GetLabels(ctx context.Context, q *prompb.Query, labelDBUrl string, originalJobLabel string) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries

	m, err := parseQueryMatchers(q.Matchers)
	if err != nil {
		return nil, fmt.Errorf("failed to generate internal query")
	}
	matchedLabelsList, err := index.GetMatchedLabels(ctx, labelDBUrl, m, q.StartTimestampMs/1000, q.EndTimestampMs/1000)
	if err != nil {
		return nil, fmt.Errorf("failed to generate internal query")
	}
	for _, matchedLabels := range matchedLabelsList {
		ts := &prompb.TimeSeries{}
		for _, label := range matchedLabels {
			if label.Name == "MetricName" {
				continue
			}
			ts.Labels = append(ts.Labels, prompb.Label{Name: label.Name, Value: label.Value})
		}
		ts.Labels = append(ts.Labels, prompb.Label{Name: "job", Value: originalJobLabel})
		t := time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000))
		ts.Samples = append(ts.Samples, prompb.Sample{Value: 0, Timestamp: t.Unix() * 1000})
		result = append(result, ts)
	}
	//level.Debug(logger).Log("msg", "namespace is required")
	return result, nil
}

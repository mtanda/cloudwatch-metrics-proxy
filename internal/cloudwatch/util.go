package cloudwatch

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

var invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func safeMetricName(name string) string {
	if len(name) == 0 {
		return ""
	}
	name = invalidMetricNamePattern.ReplaceAllString(name, "_")
	if '0' <= name[0] && name[0] <= '9' {
		name = "_" + name
	}
	return name
}

func isSingleStatistic(queries []*cloudwatch.GetMetricStatisticsInput) bool {
	s := ""
	for _, query := range queries {
		if len(query.Statistics) > 1 || len(query.ExtendedStatistics) > 1 {
			return false
		}
		if len(query.Statistics) == 1 {
			if s == "" {
				s = string(query.Statistics[0])
				continue
			}
			if s != string(query.Statistics[0]) {
				return false
			}
		}
		if len(query.ExtendedStatistics) == 1 {
			if s == "" {
				s = query.ExtendedStatistics[0]
				continue
			}
			if s != query.ExtendedStatistics[0] {
				return false
			}
		}
	}
	return true
}

func isExtendedStatistics(s string) bool {
	return s != "Sum" && s != "SampleCount" && s != "Maximum" && s != "Minimum" && s != "Average"
}

func calcQueryPeriod(startTime time.Time, endTime time.Time, periodUnit int32) int32 {
	queryTimeRange := (endTime).Sub(startTime).Seconds()
	period := int32(math.Ceil(queryTimeRange/1440/float64(periodUnit))) * periodUnit
	if period < periodUnit {
		period = periodUnit
	}
	return period
}

func calibratePeriod(startTime time.Time) int32 {
	var period int32

	timeDay := 24 * time.Hour
	now := time.Now().UTC()
	timeRangeToNow := now.Sub(startTime)
	if timeRangeToNow < timeDay*15 { // until 15 days ago
		period = 60
	} else if timeRangeToNow <= (timeDay * 63) { // until 63 days ago
		period = 60 * 5
	} else if timeRangeToNow <= (timeDay * 455) { // until 455 days ago
		period = 60 * 60
	} else { // over 455 days, should return error, but try to long period
		period = 60 * 60
	}

	return period
}

func parseQueryMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var m *labels.Matcher
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			m = labels.MustNewMatcher(labels.MatchEqual, matcher.Name, matcher.Value)
		case prompb.LabelMatcher_NEQ:
			m = labels.MustNewMatcher(labels.MatchNotEqual, matcher.Name, matcher.Value)
		case prompb.LabelMatcher_RE:
			m = labels.MustNewMatcher(labels.MatchRegexp, matcher.Name, "^(?:"+matcher.Value+")$")
		case prompb.LabelMatcher_NRE:
			m = labels.MustNewMatcher(labels.MatchNotRegexp, matcher.Name, "^(?:"+matcher.Value+")$")
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, m)
	}
	return result, nil
}

func parseQueryParams(matchers []*prompb.LabelMatcher, maximumStep int64) ([]types.Statistic, []string, int32, error) {
	statistics := make([]types.Statistic, 0)
	extendedStatistics := make([]string, 0)
	period := int32(0)

	for _, m := range matchers {
		if !(m.Type == prompb.LabelMatcher_EQ || m.Type == prompb.LabelMatcher_RE) {
			continue // only support equal matcher or regex matcher with alternation
		}

		ss := make([]string, 0)
		for _, s := range strings.Split(m.Value, "|") {
			ss = append(ss, s)
		}

		switch m.Name {
		case "Statistic":
			for _, s := range ss {
				statistics = append(statistics, types.Statistic(s))
			}
		case "ExtendedStatistic":
			extendedStatistics = append(extendedStatistics, ss...)
		case "Period":
			if m.Type == prompb.LabelMatcher_EQ {
				v, err := strconv.ParseInt(m.Value, 10, 64)
				if err != nil {
					d, err := time.ParseDuration(m.Value)
					if err != nil {
						return nil, nil, 0, err
					}
					v = int64(d.Seconds())
				}
				maximumStep = int64(math.Max(float64(maximumStep), float64(60)))
				if v < maximumStep {
					v = maximumStep
				}
				period = int32(v)
			}
		}
	}

	return statistics, extendedStatistics, period, nil
}

var regionCache = ""

func getDefaultRegion() (string, error) {
	var region string

	if regionCache != "" {
		return regionCache, nil
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	client := imds.NewFromConfig(cfg)
	response, err := client.GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	} else {
		region = response.Region
		if region != "" {
			regionCache = region
		}
	}

	return region, nil
}

func getClient(ctx context.Context, region string) (*cloudwatch.Client, error) {
	if client, ok := clientCache[region]; ok {
		return client, nil
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	clientCache[region] = cloudwatch.NewFromConfig(awsCfg)
	return clientCache[region], nil
}

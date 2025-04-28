package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

const (
	PROMETHEUS_MAXIMUM_POINTS = 11000
	PROMETHEUS_LOOKBACK_DELTA = 5 * time.Minute
)

type adapterConfig struct {
	listenAddr string
	labelDBUrl string
}

func runQuery(ctx context.Context, q *prompb.Query, labelDBUrl string, lookbackDelta time.Duration, logger log.Logger) ([]*prompb.TimeSeries, error) {
	namespace, debugMode, originalJobLabel, matchers := parseQuery(q)
	q.Matchers = matchers

	// return label name/value list for query editor
	if namespace == "" || q.Hints == nil {
		return getLabels(ctx, q, labelDBUrl, originalJobLabel)
	}

	maximumStep := int64(math.Ceil(float64(q.Hints.StepMs) / float64(1000)))
	if maximumStep == 0 {
		maximumStep = 1 // q.Hints.StepMs == 0 in some query...
	}

	// get time series from recent time range
	result, err := runCloudWatchQuery(ctx, debugMode, logger, q, labelDBUrl, maximumStep, lookbackDelta)
	if err != nil {
		return result, err
	}

	if originalJobLabel != "" {
		for _, ts := range result {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "job", Value: originalJobLabel})
		}
	}

	if debugMode {
		level.Info(logger).Log("msg", fmt.Sprintf("Returned %d time series.", len(result)))
	}

	return result, nil
}

func runCloudWatchQuery(ctx context.Context, debugMode bool, logger log.Logger, q *prompb.Query, labelDBUrl string, maximumStep int64, lookbackDelta time.Duration) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries
	var region string
	var queries []*cloudwatch.GetMetricStatisticsInput
	var err error

	// index doesn't have statistics label, get label matchers without statistics
	mm := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Name == "Statistic" || m.Name == "ExtendedStatistic" || m.Name == "Period" {
			continue
		}
		mm = append(mm, m)
	}

	matchers, err := fromLabelMatchers(mm)
	if err != nil {
		return nil, fmt.Errorf("failed to generate internal query")
	}

	if debugMode {
		level.Info(logger).Log("msg", "querying for CloudWatch with index", "query", fmt.Sprintf("%+v", q))
	}
	region, queries, err = getQueryWithIndex(ctx, q, matchers, labelDBUrl, maximumStep)
	if err != nil {
		level.Error(logger).Log("err", err)
		return nil, fmt.Errorf("failed to generate internal query")
	}

	// if no queries are generated, try to get time series without index
	if len(queries) == 0 {
		if debugMode {
			level.Info(logger).Log("msg", "querying for CloudWatch without index", "query", fmt.Sprintf("%+v", q))
		}
		region, queries, err = getQueryWithoutIndex(q, maximumStep)
		if err != nil {
			level.Error(logger).Log("err", err)
			return nil, fmt.Errorf("failed to generate internal query")
		}
	}

	if region != "" && len(queries) > 0 {
		result, err = queryCloudWatch(ctx, region, queries, q, lookbackDelta)
		if err != nil {
			level.Error(logger).Log("err", err, "query", queries)
			return nil, fmt.Errorf("failed to get time series from CloudWatch")
		}
	}
	if debugMode {
		level.Info(logger).Log("msg", "dump query result", "result", fmt.Sprintf("%+v", result))
	}
	return result, nil
}

func parseQuery(q *prompb.Query) (string, bool, string, []*prompb.LabelMatcher) {
	namespace := ""
	debugMode := false
	originalJobLabel := ""
	matchers := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "job" {
			originalJobLabel = m.Value
			continue
		}
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "debug" {
			debugMode = true
			continue
		}
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "Namespace" {
			namespace = m.Value
		}
		matchers = append(matchers, m)
	}
	return namespace, debugMode, originalJobLabel, matchers
}

func getLabels(ctx context.Context, q *prompb.Query, labelDBUrl string, originalJobLabel string) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries

	m, err := fromLabelMatchers(q.Matchers)
	if err != nil {
		return nil, fmt.Errorf("failed to generate internal query")
	}
	matchedLabelsList, err := getMatchedLabels(ctx, labelDBUrl, m, q.StartTimestampMs/1000, q.EndTimestampMs/1000)
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

func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
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

func remoteReadHandler(ctx context.Context, cfg *adapterConfig, logger log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Queries) != 1 {
			http.Error(w, "Can only handle one query.", http.StatusBadRequest)
			return
		}

		timeSeries, err := runQuery(ctx, req.Queries[0], cfg.labelDBUrl, PROMETHEUS_LOOKBACK_DELTA, logger)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{Timeseries: timeSeries},
			},
		}
		data, err := proto.Marshal(&resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func metricsHandler(rsp http.ResponseWriter, req *http.Request) {
	httpError := func(rsp http.ResponseWriter, err error) {
		rsp.Header().Del("Content-Encoding")
		http.Error(
			rsp,
			"An error has occurred while serving metrics:\n\n"+err.Error(),
			http.StatusInternalServerError,
		)
	}

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		httpError(rsp, err)
		return
	}

	contentType := expfmt.Negotiate(req.Header)
	header := rsp.Header()
	header.Set("Content-Type", string(contentType))

	w := io.Writer(rsp)
	enc := expfmt.NewEncoder(w, contentType)

	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			httpError(rsp, err)
			return
		}
	}
}

func main() {
	var cfg adapterConfig

	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9420", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.labelDBUrl, "labeldb.address", "http://localhost:8080/", "Address of the label database.")
	flag.Parse()

	logLevel := promlog.AllowedLevel{}
	logLevel.Set("info")
	format := promlog.AllowedFormat{}
	format.Set("json")
	config := promlog.Config{Level: &logLevel, Format: &format}
	logger := promlog.New(&config)

	pctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(pctx)

	srv := &http.Server{Addr: cfg.listenAddr}
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/read", remoteReadHandler(ctx, &cfg, logger))

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(term)
		cancel()
	}()
	go func() {
		select {
		case <-term:
			level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
			cancel()
			if err := eg.Wait(); err != nil {
				level.Error(logger).Log("err", err)
			}

			ctxHttp, _ := context.WithTimeout(context.Background(), 60*time.Second)
			if err := srv.Shutdown(ctxHttp); err != nil {
				level.Error(logger).Log("err", err)
			}
		case <-pctx.Done():
		}
	}()

	level.Info(logger).Log("msg", "Listening on "+cfg.listenAddr)
	if err := srv.ListenAndServe(); err != nil {
		level.Error(logger).Log("err", err)
	}
}

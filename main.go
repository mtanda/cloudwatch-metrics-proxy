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
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

const (
	PROMETHEUS_MAXIMUM_POINTS = 11000
)

type adapterConfig struct {
	listenAddr  string
	configFile  string
	storagePath string
}

func runQuery(ctx context.Context, indexer *Indexer, q *prompb.Query, lookbackDelta time.Duration, logger log.Logger) ([]*prompb.TimeSeries, error) {
	result := make(resultMap)

	namespace := ""
	debugMode := false
	debugIndexMode := false
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
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "debug_index" {
			debugIndexMode = true
			continue
		}
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "Namespace" {
			namespace = m.Value
		}
		matchers = append(matchers, m)
	}
	q.Matchers = matchers

	// return label name/value list for query editor
	if namespace == "" || q.Hints == nil {
		m, err := fromLabelMatchers(q.Matchers)
		if err != nil {
			return nil, fmt.Errorf("failed to generate internal query")
		}
		matchedLabelsList, err := indexer.getMatchedLabels(ctx, m, q.StartTimestampMs, q.EndTimestampMs)
		if err != nil {
			return nil, fmt.Errorf("failed to generate internal query")
		}
		for i, matchedLabels := range matchedLabelsList {
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
			result[fmt.Sprint(i)] = ts
		}
		//level.Debug(logger).Log("msg", "namespace is required")
		return result.slice(), nil
	}

	endTime := time.Unix(int64(q.Hints.EndMs/1000), int64(q.Hints.EndMs%1000*1000))
	now := time.Now().UTC()
	if endTime.After(now) {
		q.Hints.EndMs = now.Unix() * 1000
		endTime = time.Unix(int64(q.Hints.EndMs/1000), int64(q.Hints.EndMs%1000*1000))
	}
	maximumStep := int64(math.Ceil(float64(q.Hints.StepMs) / float64(1000)))
	if maximumStep == 0 {
		maximumStep = 1 // q.Hints.StepMs == 0 in some query...
	}

	if debugIndexMode {
		result, err := indexer.Query(ctx, q, maximumStep, lookbackDelta)
		if err != nil {
			level.Error(logger).Log("err", err)
			return nil, fmt.Errorf("failed to get time series from index")
		}
		return result.slice(), nil
	}

	// get time series from recent time range
	if q.Hints.StartMs < q.Hints.EndMs {
		var region string
		var queries []*cloudwatch.GetMetricStatisticsInput
		var err error
		if indexer.isExpired(endTime, []string{namespace}) {
			if debugMode {
				level.Info(logger).Log("msg", "querying for CloudWatch without index", "query", fmt.Sprintf("%+v", q))
			}
			region, queries, err = getQueryWithoutIndex(q, indexer, maximumStep)
		} else {
			if debugMode {
				level.Info(logger).Log("msg", "querying for CloudWatch with index", "query", fmt.Sprintf("%+v", q))
			}
			region, queries, err = getQueryWithIndex(ctx, q, indexer, maximumStep)
		}
		if err != nil {
			level.Error(logger).Log("err", err)
			return nil, fmt.Errorf("failed to generate internal query")
		}
		if region != "" && len(queries) > 0 {
			err = queryCloudWatch(ctx, region, queries, q, lookbackDelta, result)
			if err != nil {
				level.Error(logger).Log("err", err, "query", queries)
				return nil, fmt.Errorf("failed to get time series from CloudWatch")
			}
		}
		if debugMode {
			level.Info(logger).Log("msg", "dump query result", "result", fmt.Sprintf("%+v", result))
		}
	}

	if originalJobLabel != "" {
		for _, ts := range result {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "job", Value: originalJobLabel})
		}
	}

	if debugMode {
		level.Info(logger).Log("msg", fmt.Sprintf("Returned %d time series.", len(result)))
	}

	return result.slice(), nil
}

func main() {
	var cfg adapterConfig

	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9415", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.configFile, "config.file", "./cloudwatch_read_adapter.yml", "Configuration file path.")
	flag.StringVar(&cfg.storagePath, "storage.tsdb.path", "./data", "Base path for metrics storage.")
	flag.Parse()

	logLevel := promlog.AllowedLevel{}
	logLevel.Set("info")
	format := promlog.AllowedFormat{}
	format.Set("json")
	config := promlog.Config{Level: &logLevel, Format: &format}
	logger := promlog.New(&config)

	readCfg, err := LoadConfig(cfg.configFile)
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	if len(readCfg.Targets) == 0 {
		level.Info(logger).Log("msg", "no targets")
		os.Exit(0)
	}

	// set default region
	region, err := GetDefaultRegion()
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	if len(readCfg.Targets[0].Index.Region) == 0 {
		readCfg.Targets[0].Index.Region = append(readCfg.Targets[0].Index.Region, region)
	}

	pctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(pctx)
	indexer, err := NewIndexer(readCfg.Targets[0].Index, cfg.storagePath, log.With(logger, "component", "indexer"))
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	indexer.start(eg, ctx)

	srv := &http.Server{Addr: cfg.listenAddr}
	http.HandleFunc("/metrics", func(rsp http.ResponseWriter, req *http.Request) {
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
		mfsi, err := indexer.registry.Gather()
		if err != nil {
			httpError(rsp, err)
			return
		}

		ln := "database"
		lv1 := "index"
		for _, mf := range mfsi {
			for _, m := range mf.Metric {
				m.Label = []*io_prometheus_client.LabelPair{}
				for _, l := range m.Label {
					m.Label = append(m.Label, &io_prometheus_client.LabelPair{Name: l.Name, Value: l.Value})
				}
				m.Label = append(m.Label, &io_prometheus_client.LabelPair{Name: &ln, Value: &lv1})
			}
		}
		mfs = append(mfs, mfsi...)

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
	})
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
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

		timeSeries, err := runQuery(ctx, indexer, req.Queries[0], readCfg.LookbackDelta, logger)
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
	})

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

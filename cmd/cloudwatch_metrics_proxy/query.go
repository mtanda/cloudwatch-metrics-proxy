package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/mtanda/cloudwatch_metrics_proxy/internal/cloudwatch"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	PROMETHEUS_LOOKBACK_DELTA = 5 * time.Minute
)

func runQuery(ctx context.Context, q *prompb.Query, labelDBUrl string) ([]*prompb.TimeSeries, error) {
	namespace, debugMode, originalJobLabel, matchers := parseQuery(q)
	q.Matchers = matchers

	maximumStep := int64(math.Ceil(float64(q.Hints.StepMs) / float64(1000)))
	if maximumStep == 0 {
		maximumStep = 1 // q.Hints.StepMs == 0 in some query...
	}

	cloudwatchClient, err := cloudwatch.New(labelDBUrl, maximumStep, PROMETHEUS_LOOKBACK_DELTA, *q.Hints)
	if err != nil {
		slog.Error("failed to new client", "err", err)
		return nil, fmt.Errorf("failed to new client")
	}

	// return label name/value list for query editor
	if namespace == "" || q.Hints == nil {
		return cloudwatchClient.GetLabels(ctx, q, labelDBUrl, originalJobLabel)
	}

	// get time series from recent time range
	result, err := runCloudWatchQuery(ctx, cloudwatchClient, debugMode, q, labelDBUrl, originalJobLabel)
	if err != nil {
		return result, err
	}

	return result, nil
}

func runCloudWatchQuery(ctx context.Context, cloudwatchClient *cloudwatch.CloudWatchClient, debugMode bool, q *prompb.Query, labelDBUrl string, originalJobLabel string) ([]*prompb.TimeSeries, error) {
	var result []*prompb.TimeSeries

	if debugMode {
		slog.Info("querying for CloudWatch", "query", fmt.Sprintf("%+v", q))
	}

	region, queries, err := cloudwatchClient.GetQuery(ctx, q)
	if err != nil {
		slog.Error("failed to get query", "err", err)
		return nil, fmt.Errorf("failed to generate internal query")
	}

	if region != "" && len(queries) > 0 {
		result, err = cloudwatchClient.QueryCloudWatch(ctx, region, queries)
		if err != nil {
			slog.Error("failed to execute query", "err", err, "query", queries)
			return nil, fmt.Errorf("failed to get time series from CloudWatch")
		}
	}
	if debugMode {
		slog.Info("dump query result", "result", fmt.Sprintf("%+v", result))
	}

	if originalJobLabel != "" {
		for _, ts := range result {
			ts.Labels = append(ts.Labels, prompb.Label{Name: "job", Value: originalJobLabel})
		}
	}

	if debugMode {
		slog.Info(fmt.Sprintf("Returned %d time series.", len(result)))
	}

	return result, nil
}

// get some labels from query, and remove them from matchers
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

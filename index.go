package main

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	indexInterval = time.Duration(60) * time.Minute // TODO: get from labels database
)

func getMatchedLabels(ctx context.Context, matchers []*labels.Matcher, start int64, end int64) ([]labels.Labels, error) {
	matchedLabels := make([]labels.Labels, 0)
	return matchedLabels, nil
}

func isExpired(t time.Time, namespace []string) bool {
	return false
}

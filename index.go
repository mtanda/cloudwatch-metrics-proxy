package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	indexInterval = time.Duration(60) * time.Minute // TODO: get from labels database
)

type Response struct {
	Status string   `json:"status"`
	Data   []Metric `json:"data"`
}

type Metric map[string]string

func getMatchedLabels(ctx context.Context, url string, matchers []*labels.Matcher, start int64, end int64) ([]labels.Labels, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", url+"/api/v1/series", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	q.Add("match[]", matchers[0].String())
	for _, matcher := range matchers[1:] {
		q.Add("match[]", matcher.String())
	}
	q.Add("start", strconv.FormatInt(start, 10))
	q.Add("end", strconv.FormatInt(end, 10))
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get labels: %s", resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	var response Response
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	matchedLabels := make([]labels.Labels, 0)
	for _, metric := range response.Data {
		labelsMap := labels.FromMap(metric)
		matchedLabels = append(matchedLabels, labelsMap)
	}

	return matchedLabels, nil
}

func isExpired(t time.Time, namespace []string) bool {
	return false
}

package index

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
	// GetMetricStatistics has a limit of 400 TPS per Region
	seriesLimit = 400
)

type Response struct {
	Status string   `json:"status"`
	Data   []Metric `json:"data"`
}

type Metric map[string]string

func GetMatchedLabels(ctx context.Context, url string, matchers []*labels.Matcher, start int64, end int64) ([]labels.Labels, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", url+"/api/v1/series", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	matchersStr := "{"
	for i, matcher := range matchers {
		if i > 0 {
			matchersStr += ","
		}
		matchersStr += fmt.Sprintf("%s%s\"%s\"", matcher.Name, matcher.Type, matcher.Value)
	}
	matchersStr += "}"
	q.Add("match[]", matchersStr)
	q.Add("start", strconv.FormatInt(start, 10))
	q.Add("end", strconv.FormatInt(end, 10))
	q.Add("limit", strconv.FormatInt(seriesLimit, 10))
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

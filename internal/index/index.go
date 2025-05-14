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
	seriesLimit = 1000
)

type Response struct {
	Status string   `json:"status"`
	Data   []Metric `json:"data"`
}

type Metric map[string]string

type LabelDBClient struct {
	url string
}

func New(url string) *LabelDBClient {
	return &LabelDBClient{
		url: url,
	}
}

func (l *LabelDBClient) GetMatchedLabels(ctx context.Context, matchers []*labels.Matcher, start int64, end int64, debugMode bool) ([]labels.Labels, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", l.url+"/api/v1/series", nil)
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
	if debugMode {
		q.Add("debug", "true")
	}
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

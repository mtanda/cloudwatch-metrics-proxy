package index

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestGetMatchedLabels(t *testing.T) {
	mockResponse := `{
		"status": "success",
		"data": [
			{
				"__name__": "up",
				"job": "prometheus",
				"instance": "localhost:9090"
			},
			{
				"__name__": "up",
				"job": "node",
				"instance": "localhost:9091"
			}
		]
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/series", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
	}
	start := time.Now().Add(-time.Hour).Unix()
	end := time.Now().Unix()
	matchedLabels, err := GetMatchedLabels(context.Background(), server.URL, matchers, start, end)

	assert.NoError(t, err)
	assert.Len(t, matchedLabels, 2)
	expectedLabels := []labels.Labels{
		labels.FromMap(map[string]string{"__name__": "up", "job": "prometheus", "instance": "localhost:9090"}),
		labels.FromMap(map[string]string{"__name__": "up", "job": "node", "instance": "localhost:9091"}),
	}
	for i, expected := range expectedLabels {
		assert.Equal(t, expected, matchedLabels[i])
	}
}

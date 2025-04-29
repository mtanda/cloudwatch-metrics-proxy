package main

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
)

const (
	PROMETHEUS_LOOKBACK_DELTA = 5 * time.Minute
)

func remoteReadHandler(ctx context.Context, cfg *adapterConfig) http.HandlerFunc {
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

		timeSeries, err := runQuery(ctx, req.Queries[0], cfg.labelDBUrl, PROMETHEUS_LOOKBACK_DELTA)
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

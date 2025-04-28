package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/common/promlog"
	"golang.org/x/sync/errgroup"
)

const (
	PROMETHEUS_LOOKBACK_DELTA = 5 * time.Minute
)

type adapterConfig struct {
	listenAddr string
	labelDBUrl string
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

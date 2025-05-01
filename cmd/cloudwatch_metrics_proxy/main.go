package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
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

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	pctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(pctx)

	srv := &http.Server{Addr: cfg.listenAddr}
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/read", remoteReadHandler(ctx, &cfg))

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(term)
		cancel()
	}()
	go func() {
		select {
		case <-term:
			slog.Warn("Received SIGTERM, exiting gracefully...")
			cancel()
			if err := eg.Wait(); err != nil {
				slog.Error("failed to wait", "err", err)
			}

			ctxHttp, _ := context.WithTimeout(context.Background(), 60*time.Second)
			if err := srv.Shutdown(ctxHttp); err != nil {
				slog.Error("failed to shutdown", "err", err)
			}
		case <-pctx.Done():
		}
	}()

	slog.Info("Listening on " + cfg.listenAddr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("failed to listen and serve", "err", err)
	}
}

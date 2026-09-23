package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exemt/placitum-keeper/internal/bus"
	"github.com/exemt/placitum-keeper/internal/config"
	"github.com/exemt/placitum-keeper/internal/httpapi"
	"github.com/exemt/placitum-keeper/internal/keeper"
	"github.com/exemt/placitum-keeper/internal/pulse"
	"github.com/exemt/placitum-keeper/internal/state"
	"github.com/exemt/placitum-keeper/internal/store"
	"github.com/exemt/placitum-shared/logkit"
	"github.com/exemt/placitum-shared/loglevel"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "error", err.Error())
		os.Exit(2)
	}

	level, levelErr := loglevel.Parse(cfg.LogLevel)
	if levelErr != nil {
		level = slog.LevelInfo
	}

	journal := logkit.Open(logkit.Options{Service: cfg.Name, Level: level})
	log := journal.Log

	log.Info("build", "version", version, "revision", revision)

	if levelErr != nil {
		log.Warn("WAF_KEEPER_LOG ignored", "error", levelErr.Error())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	defs, err := openDefinitions(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("definitions", "error", err.Error())
		os.Exit(1)
	}
	defer defs.Close()

	b, err := bus.Connect(cfg.NatsURL, "waf-"+cfg.Name, log)
	if err != nil {
		log.Error("bus", "url", cfg.NatsURL, "error", err.Error())
		os.Exit(1)
	}
	defer b.Close()

	journal.Attach(ctx, b.Conn())
	defer journal.Close()

	st, err := state.Open(cfg.RedisURL, 5*time.Second)
	if err != nil {
		log.Error("state", "url", cfg.RedisURL, "error", err.Error())
		os.Exit(1)
	}

	defer st.Close()

	if err := waitState(ctx, st, log); err != nil {
		log.Error("state", "url", cfg.RedisURL, "error", err.Error())
		os.Exit(1)
	}

	k := keeper.New(defs, st, b, log, keeper.Options{
		Name:        cfg.Name,
		Tick:        cfg.Tick,
		Sweep:       cfg.Sweep,
		Coalesce:    cfg.Coalesce,
		BatchMax:    cfg.BatchMax,
		QueueMax:    cfg.QueueMax,
		Pipeline:    cfg.Pipeline,
		DiffTTL:     cfg.DiffTTL,
		SnapshotTTL: cfg.SnapshotTTL,
		InlineMax:   cfg.InlineMax,
	})

	srv := &http.Server{Addr: cfg.HTTP, Handler: httpapi.Handler(k), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "error", err.Error())
			os.Exit(1)
		}
	}()

	if err := k.Start(ctx); err != nil {
		log.Error("start", "error", err.Error())
		os.Exit(1)
	}

	if err := b.Serve(k); err != nil {
		log.Error("subscribe", "error", err.Error())
		os.Exit(1)
	}

	log.Info("keeper up", "http", cfg.HTTP, "nats", cfg.NatsURL, "redis", cfg.RedisURL,
		"tick", cfg.Tick.String(), "coalesce", cfg.Coalesce.String(),
		"batch_max", cfg.BatchMax, "queue_max", cfg.QueueMax, "pipeline", cfg.Pipeline)

	id := pulse.NewID()
	beat := time.NewTicker(cfg.Pulse)
	defer beat.Stop()

	_ = pulse.Publish(b.Conn(), stamp(pulse.Build(id, cfg.Name, k)))

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = srv.Shutdown(shutdownCtx)
			cancel()

			return
		case <-beat.C:
			if err := pulse.Publish(b.Conn(), stamp(pulse.Build(id, cfg.Name, k))); err != nil {
				log.Warn("pulse", "error", err.Error())
			}
		}
	}
}

func waitState(ctx context.Context, st *state.Redis, log *slog.Logger) error {
	var last error

	for attempt := 0; attempt < 30; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := st.Ping(pingCtx)
		cancel()

		if err == nil {
			return nil
		}

		last = err
		log.Warn("state not ready", "attempt", attempt+1, "error", err.Error())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	return last
}

func openDefinitions(ctx context.Context, url string, log *slog.Logger) (*store.PG, error) {
	var last error

	for attempt := 0; attempt < 30; attempt++ {
		st, err := store.Open(ctx, url)
		if err == nil {
			return st, nil
		}

		last = err
		log.Warn("definitions not ready", "attempt", attempt+1, "error", err.Error())

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	return nil, last
}

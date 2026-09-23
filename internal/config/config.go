package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Name        string
	DatabaseURL string
	NatsURL     string
	RedisURL    string
	SnapshotTTL time.Duration
	DiffTTL     time.Duration
	InlineMax   int
	Coalesce    time.Duration
	BatchMax    int
	QueueMax    int
	Pipeline    int
	HTTP        string
	Tick        time.Duration
	Pulse       time.Duration
	Sweep       time.Duration
	LogLevel    string
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func envDuration(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s: expected a duration, got %q", name, raw)
	}

	return d, nil
}

func envInt(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: expected a positive integer, got %q", name, raw)
	}

	return n, nil
}

// envBytes reads a byte count that may be zero: zero switches a feature off.
func envBytes(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: expected a byte count, got %q", name, raw)
	}

	return n, nil
}

func Load() (Config, error) {
	c := Config{
		Name:        env("WAF_KEEPER_NAME", "keeper"),
		DatabaseURL: env("WAF_KEEPER_DATABASE_URL", "postgres://waf:waf@postgres:5432/waf"),
		NatsURL:     env("WAF_NATS_URL", env("NATS_URL", "nats://nats:4222")),
		RedisURL:    env("WAF_KEEPER_REDIS_URL", env("REDIS_INTERNAL_URL", "")),
		HTTP:        env("WAF_KEEPER_HTTP", ":8094"),
		LogLevel:    env("WAF_KEEPER_LOG", "info"),
	}

	var err error

	if c.Tick, err = envDuration("WAF_KEEPER_TICK", 2*time.Second); err != nil {
		return c, err
	}

	if c.Pulse, err = envDuration("WAF_KEEPER_PULSE", 5*time.Second); err != nil {
		return c, err
	}

	if c.Sweep, err = envDuration("WAF_KEEPER_SWEEP", time.Second); err != nil {
		return c, err
	}

	if c.SnapshotTTL, err = envDuration("WAF_KEEPER_SNAPSHOT_TTL", 30*time.Second); err != nil {
		return c, err
	}

	if c.DiffTTL, err = envDuration("WAF_KEEPER_DIFF_TTL", 90*time.Second); err != nil {
		return c, err
	}

	if c.InlineMax, err = envBytes("WAF_KEEPER_INLINE_MAX", 1024); err != nil {
		return c, err
	}

	if c.Coalesce, err = envDuration("WAF_KEEPER_COALESCE", 10*time.Millisecond); err != nil {
		return c, err
	}

	if c.BatchMax, err = envInt("WAF_KEEPER_BATCH_MAX", 10_000); err != nil {
		return c, err
	}

	if c.QueueMax, err = envInt("WAF_KEEPER_QUEUE_MAX", 50_000); err != nil {
		return c, err
	}

	if c.Pipeline, err = envInt("WAF_KEEPER_PIPELINE", 4); err != nil {
		return c, err
	}

	if c.RedisURL == "" {
		return c, fmt.Errorf("WAF_KEEPER_REDIS_URL is empty: set state lives in the internal Redis")
	}

	if c.DiffTTL < 2*c.SnapshotTTL {
		return c, fmt.Errorf("WAF_KEEPER_DIFF_TTL %s must be at least twice WAF_KEEPER_SNAPSHOT_TTL %s: "+
			"a snapshot is useless once the packages after it are gone", c.DiffTTL, c.SnapshotTTL)
	}

	return c, nil
}

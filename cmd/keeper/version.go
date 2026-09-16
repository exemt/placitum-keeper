package main

import "github.com/exemt/placitum-keeper/internal/pulse"

var (
	version  = "dev"
	revision = "unknown"
)

func stamp(m pulse.Message) pulse.Message {
	m.Version, m.Revision = version, revision

	return m
}

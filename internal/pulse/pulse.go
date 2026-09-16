package pulse

import (
	"github.com/nats-io/nats.go"

	"github.com/exemt/placitum-keeper/internal/keeper"
	shared "github.com/exemt/placitum-shared/pulse"
)

type Work struct {
	Sets       int   `json:"sets"`
	Entries    int   `json:"entries"`
	Writes     int64 `json:"writes"`
	Rejects    int64 `json:"rejects"`
	Snapshots  int64 `json:"snapshots"`
	Packs      int64 `json:"packs"`
	Expired    int64 `json:"expired"`
	Queued     int64 `json:"queued"`
	Overloaded int64 `json:"overloaded"`
	Stale      int64 `json:"stale"`
}

type Error struct {
	At     string `json:"at"`
	Msg    string `json:"msg"`
	Count  int64  `json:"count,omitempty"`
	Source string `json:"source,omitempty"`
}

type Message struct {
	shared.Frame
	Work   Work             `json:"work"`
	Sets   []keeper.SetStat `json:"sets,omitempty"`
	Errors []Error          `json:"errors,omitempty"`
}

func NewID() string {
	return shared.NewID()
}

func Subject(name, id string) string {
	return shared.ServiceSubject(name, id)
}

func Build(id, name string, k *keeper.Keeper) Message {
	stats := k.Stats()

	msg := Message{
		Frame: shared.NewFrame("service", id, name, k.Ready(), nil),
		Sets:  stats,
	}

	msg.Work.Sets = len(stats)

	for _, s := range stats {
		msg.Work.Entries += s.Entries
		msg.Work.Writes += s.Writes
		msg.Work.Rejects += s.Rejects
		msg.Work.Snapshots += s.Snapshots
		msg.Work.Packs += s.Packs
		msg.Work.Expired += s.Expired
		msg.Work.Queued += s.Queued
		msg.Work.Overloaded += s.Overloaded
		msg.Work.Stale += s.Stale

		if s.StoreErr > 0 {
			msg.Errors = append(msg.Errors, Error{
				At:     msg.At,
				Msg:    "state commit failed: " + s.Name,
				Count:  s.StoreErr,
				Source: "redis-internal",
			})
		}
	}

	return msg
}

func Publish(nc *nats.Conn, msg Message) error {
	return shared.PublishFrame(nc, Subject(msg.Name, msg.ID), msg)
}

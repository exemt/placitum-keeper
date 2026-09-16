package bus

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/exemt/placitum-keeper/internal/keeper"
	"github.com/exemt/placitum-keeper/internal/wire"
)

const (
	prefix        = "waf.sets."
	eventTimeout  = 5 * time.Second
	eventInflight = 1024
)

type Bus struct {
	nc    *nats.Conn
	log   *slog.Logger
	slots chan struct{}
}

func Connect(url, name string, log *slog.Logger) (*Bus, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("bus disconnected", "error", err.Error())
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("bus reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Bus{nc: nc, log: log, slots: make(chan struct{}, eventInflight)}, nil
}

func (b *Bus) Conn() *nats.Conn { return b.nc }

func (b *Bus) Close() { b.nc.Drain() }

func (b *Bus) Publish(setName string, body []byte) {
	if err := b.nc.Publish(prefix+setName, body); err != nil {
		b.log.Warn("publish failed", "set", setName, "error", err.Error())
	}
}

func setOf(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) < 3 {
		return ""
	}

	return parts[2]
}

func (b *Bus) reply(msg *nats.Msg, v any) {
	if msg.Reply == "" {
		return
	}

	if err := msg.Respond(wire.Encode(v)); err != nil {
		b.log.Warn("reply failed", "subject", msg.Subject, "error", err.Error())
	}
}

func (b *Bus) Serve(k *keeper.Keeper) error {
	subs := []struct {
		subject string
		handler nats.MsgHandler
	}{
		{prefix + "*.event", b.onEvent(k)},
		{prefix + "*.snapshot", b.onSnapshot(k)},
		{"waf.keeper.define", b.onDefine(k)},
		{"waf.keeper.reload", b.onReload(k)},
		{"waf.keeper.lookup", b.onLookup(k)},
	}

	for _, s := range subs {
		if _, err := b.nc.Subscribe(s.subject, s.handler); err != nil {
			return err
		}
	}

	return b.nc.Flush()
}

func (b *Bus) onEvent(k *keeper.Keeper) nats.MsgHandler {
	return func(msg *nats.Msg) {
		select {
		case b.slots <- struct{}{}:
		default:
			b.reply(msg, wire.EventReply{Error: wire.ErrOverloaded})

			return
		}

		go func() {
			defer func() { <-b.slots }()

			var ev wire.Event
			if err := json.Unmarshal(msg.Data, &ev); err != nil {
				b.log.Warn("event is not json", "subject", msg.Subject)
				b.reply(msg, wire.EventReply{Error: wire.ErrBadRequest})

				return
			}

			name := setOf(msg.Subject)
			if name == "" {
				name = ev.Set
			}

			ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
			defer cancel()

			r := k.Submit(ctx, name, ev)

			if r.Error != "" {
				b.log.Debug("event rejected", "set", name, "origin", ev.Origin, "error", r.Error)
			}

			b.reply(msg, r)
		}()
	}
}

func (b *Bus) onSnapshot(k *keeper.Keeper) nats.MsgHandler {
	return func(msg *nats.Msg) {
		go func() {
			var req wire.SnapshotRequest
			_ = json.Unmarshal(msg.Data, &req)

			b.reply(msg, k.Snapshot(setOf(msg.Subject), req))
		}()
	}
}

func (b *Bus) onDefine(k *keeper.Keeper) nats.MsgHandler {
	return func(msg *nats.Msg) {
		go func() {
			var req wire.DefineRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil || req.Name == "" {
				b.reply(msg, wire.DefineReply{Error: wire.ErrBadRequest})

				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			b.reply(msg, k.Define(ctx, req.Name))
		}()
	}
}

func (b *Bus) onReload(k *keeper.Keeper) nats.MsgHandler {
	return func(msg *nats.Msg) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			if err := k.Reload(ctx); err != nil {
				b.log.Error("reload failed", "error", err.Error())
				b.reply(msg, wire.DefineReply{Error: wire.ErrStoreUnavailable})

				return
			}

			b.reply(msg, wire.DefineReply{OK: true})
		}()
	}
}

func (b *Bus) onLookup(k *keeper.Keeper) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var req wire.LookupRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.Set == "" {
			b.reply(msg, wire.LookupReply{Error: wire.ErrBadRequest})

			return
		}

		b.reply(msg, k.Lookup(req.Set, req.Value))
	}
}

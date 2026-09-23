package wire

import (
	"encoding/json"
	"fmt"

	"github.com/exemt/placitum-keeper/internal/set"
)

const (
	V3 = 3

	OpDiff     = "diff"
	OpSnapshot = "snapshot"
	OpTick     = "tick"

	OpNotReady         = "not_ready"
	OpUnknownSet       = "unknown_set"
	OpStoreUnavailable = "store_unavailable"
)

const (
	ErrUnknownSet       = "unknown_set"
	ErrFull             = "full"
	ErrWrongType        = "wrong_type"
	ErrTooLong          = "too_long"
	ErrNoOrigin         = "no_origin"
	ErrNoTTL            = "no_ttl"
	ErrBadOp            = "bad_op"
	ErrBadRequest       = "bad_request"
	ErrStoreUnavailable = "store_unavailable"
	ErrNotReady         = "not_ready"
	ErrOverloaded       = "overloaded"
	ErrForever          = "forever"
)

type Frame struct {
	V       int    `json:"v"`
	Set     string `json:"set"`
	Epoch   string `json:"epoch"`
	Seq     uint64 `json:"seq"`
	Op      string `json:"op"`
	Hash    string `json:"hash,omitempty"`
	Key     string `json:"key,omitempty"`
	Package string `json:"package,omitempty"`
	Inline  string `json:"inline,omitempty"`
	Object  string `json:"object,omitempty"`
	Count   int    `json:"count,omitempty"`
	Bytes   int    `json:"bytes,omitempty"`
	TTL     int    `json:"ttl,omitempty"`
}

type SnapshotRequest struct {
	From string `json:"from"`
}

type Event struct {
	V       int      `json:"v"`
	Set     string   `json:"set"`
	Op      string   `json:"op"`
	Value   string   `json:"value"`
	Values  []string `json:"values,omitempty"`
	TTL     int64    `json:"ttl"`
	Origin  string   `json:"origin"`
	Reason  string   `json:"reason"`
	Forever bool     `json:"forever,omitempty"`
	Hashed  bool     `json:"hashed,omitempty"`
}

type EventReply struct {
	OK    bool   `json:"ok"`
	Seq   uint64 `json:"seq,omitempty"`
	Epoch string `json:"epoch,omitempty"`
	Error string `json:"error,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type DefineRequest struct {
	Name string `json:"name"`
}

type DefineReply struct {
	OK    bool   `json:"ok"`
	Epoch string `json:"epoch,omitempty"`
	Error string `json:"error,omitempty"`
}

type LookupRequest struct {
	Set   string `json:"set"`
	Value string `json:"value"`
}

type LookupReply struct {
	OK    bool   `json:"ok"`
	Found bool   `json:"found"`
	Exp   int64  `json:"exp,omitempty"`
	Error string `json:"error,omitempty"`
}

func Hex(v uint64) string { return fmt.Sprintf("%016x", v) }

func KeyHex(k set.Key) string { return fmt.Sprintf("%x", k[:]) }

func Encode(v any) []byte {
	b, _ := json.Marshal(v)

	return b
}

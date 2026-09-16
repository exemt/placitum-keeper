package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/exemt/placitum-keeper/internal/keeper"
	"github.com/exemt/placitum-keeper/internal/wire"
)

func Handler(k *keeper.Keeper) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !k.Ready() {
			http.Error(w, "loading", http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /sets", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(k.Stats())
	})

	mux.HandleFunc("GET /sets/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		ref := k.Snapshot(r.PathValue("name"), wire.SnapshotRequest{From: "http"})

		if ref.Op == wire.OpUnknownSet {
			w.WriteHeader(http.StatusNotFound)
		}

		_ = json.NewEncoder(w).Encode(ref)
	})

	return mux
}

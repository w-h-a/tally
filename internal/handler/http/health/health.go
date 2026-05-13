package health

import (
	"net/http"
	"strings"

	httphandler "github.com/w-h-a/tally/internal/handler/http"
)

type Handler struct {
	version   string
	stateFunc func() string
}

func New(version string, stateFunc func() string) *Handler {
	return &Handler{
		version:   version,
		stateFunc: stateFunc,
	}
}

func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{
		"status":  "ok",
		"version": h.version,
	}

	if h.stateFunc != nil {
		resp["state"] = strings.ToLower(h.stateFunc())
	}

	httphandler.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{
		"version": h.version,
	}

	if h.stateFunc == nil {
		resp["status"] = "ready"
		httphandler.WriteJSON(w, http.StatusOK, resp)
		return
	}

	state := strings.ToLower(h.stateFunc())
	resp["state"] = state

	if state == "leader" || state == "follower" {
		resp["status"] = "ready"
		httphandler.WriteJSON(w, http.StatusOK, resp)
		return
	}

	resp["status"] = "not_ready"
	httphandler.WriteJSON(w, http.StatusServiceUnavailable, resp)
}

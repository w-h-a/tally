package health

import (
	"fmt"
	"net/http"
)

type Handler struct{}

func New() *Handler {
	return &Handler{}
}

func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

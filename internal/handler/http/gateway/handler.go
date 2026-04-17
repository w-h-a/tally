package gateway

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	gatewayapi "github.com/w-h-a/tally/api/gateway/v1"
	httphandler "github.com/w-h-a/tally/internal/handler/http"
	api "github.com/w-h-a/tally/proto/log/v1"
)

type Handler struct {
	client api.LogServiceClient
}

func New(client api.LogServiceClient) *Handler {
	return &Handler{client: client}
}

func (h *Handler) Produce(w http.ResponseWriter, r *http.Request) {
	var req gatewayapi.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httphandler.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	resp, err := h.client.Produce(r.Context(), &api.ProduceRequest{
		Record: &api.Record{Value: []byte(req.Value)},
	})
	if err != nil {
		httphandler.WriteGRPCError(w, err)
		return
	}

	httphandler.WriteJSON(w, http.StatusOK, gatewayapi.ProduceResponse{Offset: resp.Offset})
}

func (h *Handler) Consume(w http.ResponseWriter, r *http.Request) {
	var req gatewayapi.ConsumeRequest

	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		httphandler.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "offset query parameter is required"})
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		httphandler.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "offset must be a non-negative integer"})
		return
	}

	req.Offset = offset

	resp, err := h.client.Consume(r.Context(), &api.ConsumeRequest{Offset: req.Offset})
	if err != nil {
		httphandler.WriteGRPCError(w, err)
		return
	}

	httphandler.WriteJSON(w, http.StatusOK, gatewayapi.ConsumeResponse{
		Offset: resp.Record.Offset,
		Value:  string(resp.Record.Value),
	})
}

func (h *Handler) Stream(w http.ResponseWriter, r *http.Request) {
	var req gatewayapi.StreamRequest

	if s := r.URL.Query().Get("from"); s != "" {
		from, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			httphandler.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "from must be a non-negative integer"})
			return
		}
		req.From = from
	}

	stream, err := h.client.ConsumeStream(r.Context(), &api.ConsumeStreamRequest{Offset: req.From})
	if err != nil {
		httphandler.WriteGRPCError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		httphandler.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		resp, err := stream.Recv()
		if err != nil {
			return
		}

		data, err := json.Marshal(gatewayapi.ConsumeResponse{
			Offset: resp.Record.Offset,
			Value:  string(resp.Record.Value),
		})
		if err != nil {
			slog.Error("marshal SSE event", "error", err)
			return
		}

		fmt.Fprintf(w, "id: %d\ndata: %s\n\n", resp.Record.Offset, data)
		flusher.Flush()
	}
}

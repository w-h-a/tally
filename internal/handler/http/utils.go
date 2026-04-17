package httphandler

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func WriteGRPCError(w http.ResponseWriter, err error) {
	st := status.Convert(err)

	switch st.Code() {
	case codes.NotFound:
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": st.Message()})
	case codes.InvalidArgument:
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": st.Message()})
	default:
		WriteJSON(w, http.StatusBadGateway, map[string]string{"error": st.Message()})
	}
}

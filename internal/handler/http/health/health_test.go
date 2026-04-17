package health_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/tally/internal/handler/http/health"
)

func TestHealthz(t *testing.T) {
	// arrange
	h := health.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.Healthz)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, "ok", string(body))
}

func TestReadyz(t *testing.T) {
	// arrange
	h := health.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", h.Readyz)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL + "/readyz")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, "ok", string(body))
}

package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/w-h-a/tally/internal/handler/http/health"
)

func TestHealthz(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", func() string { return "Leader" })
	srv := httptest.NewServer(http.HandlerFunc(h.Healthz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ok", body["status"])
	require.Equal(t, "v1.2.3", body["version"])
	require.Equal(t, "leader", body["state"])
}

func TestReadyz_Leader(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", func() string { return "Leader" })
	srv := httptest.NewServer(http.HandlerFunc(h.Readyz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ready", body["status"])
	require.Equal(t, "leader", body["state"])
}

func TestReadyz_Follower(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", func() string { return "Follower" })
	srv := httptest.NewServer(http.HandlerFunc(h.Readyz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ready", body["status"])
	require.Equal(t, "follower", body["state"])
}

func TestReadyz_Candidate(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", func() string { return "Candidate" })
	srv := httptest.NewServer(http.HandlerFunc(h.Readyz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "not_ready", body["status"])
	require.Equal(t, "candidate", body["state"])
}

func TestReadyz_Shutdown(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", func() string { return "Shutdown" })
	srv := httptest.NewServer(http.HandlerFunc(h.Readyz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "not_ready", body["status"])
	require.Equal(t, "shutdown", body["state"])
}

func TestReadyz_NilStateFunc(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", nil)
	srv := httptest.NewServer(http.HandlerFunc(h.Readyz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ready", body["status"])
}

func TestHealthz_NilStateFunc(t *testing.T) {
	// arrange
	h := health.New("v1.2.3", nil)
	srv := httptest.NewServer(http.HandlerFunc(h.Healthz))
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ok", body["status"])
	require.Equal(t, "v1.2.3", body["version"])
	require.Empty(t, body["state"])
}

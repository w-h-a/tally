package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/tabwriter"

	gatewayapi "github.com/w-h-a/tally/api/gateway/v1"
)

type Handler struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string, client *http.Client) *Handler {
	return &Handler{baseURL: baseURL, client: client}
}

func (h *Handler) Produce(ctx context.Context, record string, w io.Writer) error {
	body := fmt.Sprintf(`{"value":%q}`, record)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/produce", strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ReadError(resp)
	}

	var out gatewayapi.ProduceResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}

	fmt.Fprintf(w, "%d\n", out.Offset)
	return nil
}

func (h *Handler) Consume(ctx context.Context, offset uint64, w io.Writer) error {
	url := fmt.Sprintf("%s/consume?offset=%d", h.baseURL, offset)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ReadError(resp)
	}

	var out gatewayapi.ConsumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}

	fmt.Fprintf(w, "%s\n", out.Value)
	return nil
}

func (h *Handler) Servers(ctx context.Context, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+"/servers", nil)
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ReadError(resp)
	}

	var out gatewayapi.GetServersResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, s := range out.Servers {
		role := "follower"
		if s.IsLeader {
			role = "leader"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", s.ID, s.RpcAddr, role)
	}
	return tw.Flush()
}

func (h *Handler) Stream(ctx context.Context, from uint64, w io.Writer) error {
	url := fmt.Sprintf("%s/stream?from=%d", h.baseURL, from)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ReadError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		var rec gatewayapi.ConsumeResponse
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &rec); err != nil {
			return err
		}

		fmt.Fprintf(w, "%s\n", rec.Value)
	}
	return scanner.Err()
}

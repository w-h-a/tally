package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func ReadError(resp *http.Response) error {
	var e struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return fmt.Errorf("%s", e.Error)
}

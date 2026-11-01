package seaweedstore

import (
	"context"
	"encoding/json"
	"net/http"
)

type master struct {
	addr string // host:port
}

func (m master) url(path string) string {
	return "http://" + m.addr + path
}

// getJSON sends a GET request for path to a master, and decodes a 200 answer into v. Other statuses
// give an *HTTPError. Options.Timeout limits the whole request, because master answers are small.
func (c *Cluster) getJSON(ctx context.Context, m master, path string, v any) error {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	res, err := c.send(ctx, http.MethodGet, m.url(path), nil, nil, 0, 0)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return newHTTPError(res)
	}
	return json.NewDecoder(res.Body).Decode(v)
}

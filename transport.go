package vclnet

import (
	"context"
	"net"
	"net/http"
	"time"
)

// Transport returns an *http.Transport configured to dial via VPP.
// It supports connection pooling and keep-alive, matching the behavior
// of http.DefaultTransport but routed through VPP's user-space stack.
//
// Usage:
//
//	client := &http.Client{Transport: vclnet.Transport(nil)}
//	resp, err := client.Get("http://10.0.0.1:8080/health")
func Transport(d *Dialer) *http.Transport {
	if d == nil {
		d = &Dialer{}
	}
	return &http.Transport{
		DialContext:           d.dialContextForHTTP,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
	}
}

// DefaultTransport is a pre-configured *http.Transport that dials via VPP
// with connection pooling and keep-alive enabled.
var DefaultTransport = Transport(nil)

// NewHTTPClient returns an *http.Client that uses VPP for all connections.
// Connections are pooled and reused across requests (keep-alive).
func NewHTTPClient() *http.Client {
	return &http.Client{Transport: DefaultTransport}
}

func (d *Dialer) dialContextForHTTP(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.DialContext(ctx, network, addr)
}

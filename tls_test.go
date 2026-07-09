package vclnet

// Unit tests for native VCL TLS support that do not require a live VPP.
// End-to-end handshake and cert propagation are covered by
// integration_test.go's TestNativeVCLTLS* group.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestTLSConfigValidateServerRequiresCertKey documents the ListenTLS
// contract: servers cannot start without a certificate/key pair.
func TestTLSConfigValidateServerRequiresCertKey(t *testing.T) {
	cases := []struct {
		name string
		cfg  *TLSConfig
	}{
		{"nil config", nil},
		{"empty config", &TLSConfig{}},
		{"missing key", &TLSConfig{Cert: []byte("PEM")}},
		{"missing cert", &TLSConfig{Key: []byte("PEM")}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate(true)
			if err == nil {
				t.Fatal("validate(server) accepted an invalid TLSConfig")
			}
		})
	}
}

// TestTLSConfigValidateServerAcceptsFullPair ensures the happy path.
func TestTLSConfigValidateServerAcceptsFullPair(t *testing.T) {
	cfg := &TLSConfig{Cert: []byte("cert"), Key: []byte("key")}
	if err := cfg.validate(true); err != nil {
		t.Fatalf("validate(server) rejected valid pair: %v", err)
	}
}

// TestTLSConfigValidateClientAllowsAnonymous verifies that clients can dial
// TLS without any local certificate/key; VPP's default anonymous ckpair is
// used in that case.
func TestTLSConfigValidateClientAllowsAnonymous(t *testing.T) {
	if err := (*TLSConfig)(nil).validate(false); err != nil {
		t.Errorf("nil config on client err=%v, want nil", err)
	}
	if err := (&TLSConfig{}).validate(false); err != nil {
		t.Errorf("empty config on client err=%v, want nil", err)
	}
}

// TestTLSConfigValidateRejectsPartialClientCert verifies that a partial
// (cert without key or vice versa) TLSConfig is rejected on both sides.
func TestTLSConfigValidateRejectsPartialClientCert(t *testing.T) {
	only := &TLSConfig{Cert: []byte("cert")}
	if err := only.validate(false); !errors.Is(err, ErrTLSPartialCert) {
		t.Errorf("client cert-only err=%v, want ErrTLSPartialCert", err)
	}
}

// TestTLSConfigResolveCKPairSkipsWhenEmpty ensures the "anonymous client"
// path does not touch VPP: resolveCKPair returns (0, false, nil) so the
// dispatcher passes ckpValid=false into vls_connect and vppcom applies its
// default ckpair (index 0).
func TestTLSConfigResolveCKPairSkipsWhenEmpty(t *testing.T) {
	for _, cfg := range []*TLSConfig{nil, {}} {
		idx, ok, err := cfg.resolveCKPair()
		if err != nil {
			t.Errorf("resolveCKPair(%v) err=%v, want nil", cfg, err)
		}
		if ok {
			t.Errorf("resolveCKPair(%v) has-ckpair=true, want false (anonymous)", cfg)
		}
		if idx != 0 {
			t.Errorf("resolveCKPair(%v) idx=%d, want 0", cfg, idx)
		}
	}
}

// TestCKPairKeyDedup verifies the hash-based dedup key is stable across
// equal inputs and distinct across small perturbations.
func TestCKPairKeyDedup(t *testing.T) {
	cert1 := []byte("-----BEGIN CERTIFICATE-----AAAA-----END CERTIFICATE-----")
	key1 := []byte("-----BEGIN PRIVATE KEY-----BBBB-----END PRIVATE KEY-----")
	if ckpairKey(cert1, key1) != ckpairKey(cert1, key1) {
		t.Fatal("ckpairKey not stable for identical inputs")
	}
	if ckpairKey(cert1, key1) == ckpairKey(key1, cert1) {
		// The length-prefixing must prevent a naive concat collision.
		t.Fatal("ckpairKey collided across swapped cert/key")
	}
	if ckpairKey(cert1, key1) == ckpairKey(append(cert1, 'X'), key1) {
		t.Fatal("ckpairKey collided across cert content change")
	}
}

// TestDialTLSRejectsUDPNetworks locks in that vclnet.DialTLS only accepts
// tcp networks. UDP would need DTLS (a separate VPP protocol) which we do
// not currently expose.
func TestDialTLSRejectsUDPNetworks(t *testing.T) {
	for _, network := range []string{"udp", "udp4", "udp6"} {
		_, err := DialTLS(network, "127.0.0.1:443", &TLSConfig{Cert: []byte("c"), Key: []byte("k")})
		if err == nil {
			t.Errorf("DialTLS(%q) accepted UDP", network)
			continue
		}
		if !strings.Contains(err.Error(), "TLS is only supported for tcp") {
			t.Errorf("DialTLS(%q) err=%v, want UDP rejection", network, err)
		}
	}
}

// TestDialTLSRejectsUnknownNetworks ensures parseNetwork is applied before
// we start reaching for VPP.
func TestDialTLSRejectsUnknownNetworks(t *testing.T) {
	_, err := DialTLS("unix", "/tmp/nope", nil)
	if err == nil {
		t.Fatal("DialTLS(unix) accepted an unknown network")
	}
}

// TestDialTLSHonorsCanceledContext exercises the ctx.Err() short-circuit
// path in DialTLSContext; it must fail without touching VPP.
func TestDialTLSHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := DialTLSContext(ctx, "tcp4", "127.0.0.1:443", nil)
	if err == nil {
		t.Fatal("DialTLSContext(canceled) accepted the connect")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("DialTLSContext err=%v, want wraps context.Canceled", err)
	}
}

// TestListenTLSRejectsUDPNetworks locks in that ListenTLS refuses UDP.
func TestListenTLSRejectsUDPNetworks(t *testing.T) {
	cfg := &TLSConfig{Cert: []byte("c"), Key: []byte("k")}
	for _, network := range []string{"udp", "udp4", "udp6"} {
		_, err := ListenTLS(network, "127.0.0.1:0", cfg)
		if err == nil {
			t.Errorf("ListenTLS(%q) accepted UDP", network)
		}
	}
}

// TestListenTLSRejectsMissingCert ensures the server-side validate() is
// applied.
func TestListenTLSRejectsMissingCert(t *testing.T) {
	_, err := ListenTLS("tcp4", "127.0.0.1:0", nil)
	if err == nil {
		t.Fatal("ListenTLS(nil cfg) accepted request")
	}
	if !errors.Is(err, ErrTLSMissingCert) {
		t.Errorf("ListenTLS err=%v, want ErrTLSMissingCert", err)
	}
}

// TestListenTLSRejectsPartialConfig covers the "cert-only" / "key-only"
// misconfiguration paths that should be caught before we ever call into
// vclpoll.
func TestListenTLSRejectsPartialConfig(t *testing.T) {
	// Empty pair triggers the "missing cert" branch (both are absent) —
	// covered above. This test focuses on the half-populated shapes.
	cases := []struct {
		name string
		cfg  *TLSConfig
		want error
	}{
		{"cert only", &TLSConfig{Cert: []byte("c")}, ErrTLSMissingCert},
		{"key only", &TLSConfig{Key: []byte("k")}, ErrTLSMissingCert},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ListenTLS("tcp4", "127.0.0.1:0", tt.cfg)
			if err == nil {
				t.Fatalf("ListenTLS accepted %s config", tt.name)
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("err=%v, want %v", err, tt.want)
			}
		})
	}
}

// TestPutUint64BEBigEndianContract locks in the byte order used by ckpairKey
// to length-prefix cert and key blobs.
func TestPutUint64BEBigEndianContract(t *testing.T) {
	var b [8]byte
	putUint64BE(b[:], 0x0102030405060708)
	want := [8]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if b != want {
		t.Fatalf("putUint64BE = %x, want %x", b, want)
	}
}

package vclnet

// This file implements native VCL TLS support via VPPCOM_PROTO_TLS.
//
// VPP itself owns the TLS state machine when a session is created with
// PROTO_TLS: the application registers a cert/key pair once with
// vppcom_add_cert_key_pair (returning a process-global index), attaches that
// index to each TLS session via VPPCOM_ATTR_SET_CKPAIR, and then performs
// bind/listen or connect exactly as with plain TCP. Reads and writes on the
// resulting session carry cleartext application data; the ciphertext lives
// inside VPP's SVM FIFOs alongside the TLS engine (OpenSSL by default).
//
// Compared to layered crypto/tls this trades general‑purpose flexibility
// (SNI matching, verify hooks, session ticket lifetimes, key logging, ALPN)
// for one less handshake→copy round trip per record — everything stays
// inside VPP. The TLSConfig here intentionally keeps the initial surface
// small and matches what SET_CKPAIR reaches from libvppcom. Richer knobs
// (SNI, ALPN, cert verification) would require SET_ENDPT_EXT_CFG plumbing
// and are tracked in the pending‑work list.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// TLSConfig configures native VCL TLS. Cert and Key are PEM‑encoded blobs
// that are handed to VPP's cert store via vppcom_add_cert_key_pair; the
// resulting ckpair index is cached on the value so repeated Dial/Listen
// calls with the same TLSConfig registers the pair with VPP exactly once.
//
// Callers should populate a TLSConfig by value or share a pointer to a
// single instance across concurrent Dial/Listen calls — the internal cache
// is safe for concurrent use.
//
// Zero-value TLSConfig (Cert and Key both nil) is valid for clients that
// accept VPP's default anonymous ckpair; it is invalid for ListenTLS,
// which requires a server certificate.
type TLSConfig struct {
	// Cert is the PEM‑encoded certificate (leaf plus any intermediates).
	// Required for ListenTLS; optional for DialTLS (mTLS scenarios).
	Cert []byte

	// Key is the PEM‑encoded private key matching Cert. Required whenever
	// Cert is set.
	Key []byte

	// once/idx/err lazily hold the ckpair index returned by
	// vppcom_add_cert_key_pair for this config's cert+key contents. Guarded
	// by once so concurrent DialTLS/ListenTLS callers register the pair
	// only once even without external synchronisation.
	once sync.Once
	idx  uint32
	err  error
}

// ErrTLSMissingCert is returned by ListenTLS when the TLSConfig has no
// certificate or key. Servers must present a certificate; there is no
// server‑side equivalent of an anonymous client.
var ErrTLSMissingCert = errors.New("vclnet: TLSConfig requires Cert and Key for server side")

// ErrTLSPartialCert is returned when exactly one of Cert or Key is set.
var ErrTLSPartialCert = errors.New("vclnet: TLSConfig.Cert and TLSConfig.Key must both be set")

// ckpairCache dedupes ckpair registration across independent TLSConfig
// values whose cert+key bytes are identical. Without this, every fresh
// TLSConfig allocation would consume a new slot in VPP's cert store, which
// grows the pool without bound over a process lifetime.
var (
	ckpairCacheMu sync.Mutex
	ckpairCache   = map[string]uint32{}
)

// ckpairKey computes a stable key for (cert, key) so we can share ckpair
// indexes across TLSConfig instances. SHA-256 is sufficient here — the goal
// is de-duplication, not authentication.
func ckpairKey(cert, key []byte) string {
	h := sha256.New()
	// Length-prefix each field so distinct cert/key splits cannot collide.
	var l [8]byte
	putUint64BE(l[:], uint64(len(cert)))
	h.Write(l[:])
	h.Write(cert)
	putUint64BE(l[:], uint64(len(key)))
	h.Write(l[:])
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}

func putUint64BE(dst []byte, v uint64) {
	_ = dst[7]
	dst[0] = byte(v >> 56)
	dst[1] = byte(v >> 48)
	dst[2] = byte(v >> 40)
	dst[3] = byte(v >> 32)
	dst[4] = byte(v >> 24)
	dst[5] = byte(v >> 16)
	dst[6] = byte(v >> 8)
	dst[7] = byte(v)
}

// validate enforces the internal shape rules that both client and server
// paths share. serverSide requires cert+key to be present; clientSide
// allows them to be entirely absent (anonymous client) but rejects a
// partial pair.
func (c *TLSConfig) validate(serverSide bool) error {
	if c == nil {
		if serverSide {
			return ErrTLSMissingCert
		}
		return nil
	}
	hasCert := len(c.Cert) > 0
	hasKey := len(c.Key) > 0
	if serverSide && (!hasCert || !hasKey) {
		return ErrTLSMissingCert
	}
	if hasCert != hasKey {
		return ErrTLSPartialCert
	}
	return nil
}

// resolveCKPair returns (ckpairIndex, hasCKPair, error). hasCKPair is false
// when the caller supplied no cert/key (client anonymous mode); in that
// case VPP will fall back to its default ckpair (index 0). Otherwise the
// pair is registered exactly once with vppcom_add_cert_key_pair and cached
// for reuse.
func (c *TLSConfig) resolveCKPair() (uint32, bool, error) {
	if c == nil || (len(c.Cert) == 0 && len(c.Key) == 0) {
		return 0, false, nil
	}
	c.once.Do(func() {
		key := ckpairKey(c.Cert, c.Key)
		ckpairCacheMu.Lock()
		if idx, ok := ckpairCache[key]; ok {
			ckpairCacheMu.Unlock()
			c.idx = idx
			return
		}
		ckpairCacheMu.Unlock()

		idx, err := vclpoll.AddCertKeyPair(c.Cert, c.Key)
		if err != nil {
			c.err = err
			return
		}

		ckpairCacheMu.Lock()
		if existing, ok := ckpairCache[key]; ok {
			// Rare race: another goroutine registered the same bytes
			// concurrently. Release ours to keep the cert store tidy and
			// prefer the earlier index.
			ckpairCacheMu.Unlock()
			_ = vclpoll.DelCertKeyPair(idx)
			c.idx = existing
			return
		}
		ckpairCache[key] = idx
		ckpairCacheMu.Unlock()
		c.idx = idx
	})
	if c.err != nil {
		return 0, false, c.err
	}
	return c.idx, true, nil
}

// ListenTLS announces on the local network address using native VCL TLS
// (VPPCOM_PROTO_TLS). Accepted connections are already TLS‑encrypted; do
// not layer crypto/tls on top of them.
//
// The network must be "tcp", "tcp4", or "tcp6". cfg.Cert and cfg.Key are
// mandatory; the private key must match the certificate.
func ListenTLS(network, address string, cfg *TLSConfig) (net.Listener, error) {
	if shutdownStarted.Load() {
		return nil, opError("listen", network, address, ErrClosed)
	}
	if err := cfg.validate(true); err != nil {
		return nil, opError("listen", network, address, err)
	}

	_, ipv6Only, err := parseNetwork(network)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}
	if isUDP(network) {
		return nil, opError("listen", network, address, net.UnknownNetworkError(network))
	}

	addr, err := resolveAddr(network, address)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}
	if addr.Port == 0 {
		return nil, opError("listen", network, address, &net.AddrError{Err: "port 0 is not supported by VCL", Addr: address})
	}

	ckp, _, err := cfg.resolveCKPair()
	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	var vlsh vclpoll.VLSH

	if addr.IP.To4() != nil && !ipv6Only {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		vlsh, err = vclpoll.ListenTLS4(ip4, uint16(addr.Port), defaultBacklog, ckp)
	} else {
		var ip6 [16]byte
		copy(ip6[:], addr.IP.To16())
		vlsh, err = vclpoll.ListenTLS6(ip6, uint16(addr.Port), defaultBacklog, ckp)
		if err == nil && ipv6Only {
			err = vclpoll.SetV6Only(vlsh, true)
			if err != nil {
				_ = vclpoll.Close(vlsh)
			}
		}
	}

	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	info, err := vclpoll.GetLocalAddr(vlsh)
	if err != nil {
		_ = vclpoll.Close(vlsh)
		return nil, opError("listen", network, address, err)
	}
	addr = addrFromInfo(info)
	return newTCPListener(vlsh, addr, network), nil
}

// DialTLS connects to a native VCL TLS endpoint. The returned connection
// already speaks TLS at the VPP layer — do not wrap it in crypto/tls.
//
// A nil cfg or a TLSConfig with empty Cert/Key means "anonymous client TLS"
// (VPP uses its default ckpair). Providing Cert and Key configures the
// client identity for mTLS scenarios.
func DialTLS(network, address string, cfg *TLSConfig) (net.Conn, error) {
	return DialTLSContext(context.Background(), network, address, cfg)
}

// DialTLSContext is DialTLS honouring context cancellation. The context
// governs address resolution, socket creation, and the entire TLS handshake
// (VPP surfaces handshake completion as an EPOLLOUT event).
func DialTLSContext(ctx context.Context, network, address string, cfg *TLSConfig) (net.Conn, error) {
	if shutdownStarted.Load() {
		return nil, opError("dial", network, address, ErrClosed)
	}
	if err := cfg.validate(false); err != nil {
		return nil, opError("dial", network, address, err)
	}
	if _, _, err := parseNetwork(network); err != nil {
		return nil, opError("dial", network, address, err)
	}
	if isUDP(network) {
		return nil, opError("dial", network, address, fmt.Errorf("vclnet: TLS is only supported for tcp networks (got %q)", network))
	}
	if err := ctx.Err(); err != nil {
		return nil, opError("dial", network, address, err)
	}

	ckp, ckpValid, err := cfg.resolveCKPair()
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	// TLS Dial does not (yet) run through Happy Eyeballs: initial callers
	// use a specific host or a resolved single-family address. If we later
	// want dual-stack racing we can factor Dialer.dialTCP to accept a
	// per-family "start" function.
	addr, err := resolveAddrContext(ctx, network, address)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	vlsh, immediate, err := connectTLSStart(addr, ckp, ckpValid)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	if !immediate {
		// The wait covers the TLS handshake — VPP does not signal EPOLLOUT
		// until the negotiated session is fully ready for application
		// data.  On a slow handshake this is where the caller's context
		// deadline actually kicks in.
		if ok := vclpoll.PollWaitContext(vlsh, 0x004, ctx.Done()); !ok {
			vclpoll.CloseVLSH(vlsh)
			return nil, opError("dial", network, address, interruptedConnectError(ctx))
		}
	}
	if err := ctx.Err(); err != nil {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, err)
	}
	if shutdownStarted.Load() {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, ErrClosed)
	}

	conn := newTCPConn(vlsh)
	conn.peerAddr = addr
	return conn, nil
}

// connectTLSStart initiates a non-blocking TLS handshake toward addr,
// splitting the IPv4/IPv6 variants exactly as connectStart does for TCP.
func connectTLSStart(addr *net.TCPAddr, ckp uint32, ckpValid bool) (vclpoll.VLSH, bool, error) {
	if addr.IP.To4() != nil {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		return vclpoll.ConnectTLS4Start(ip4, uint16(addr.Port), ckp, ckpValid)
	}
	var ip6 [16]byte
	copy(ip6[:], addr.IP.To16())
	return vclpoll.ConnectTLS6Start(ip6, uint16(addr.Port), ckp, ckpValid)
}

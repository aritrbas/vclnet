package vclpoll

import "testing"

// Pure-Go unit tests for byte-order helpers. These do NOT touch cgo /
// VPP and so run in any environment.

func TestIPBE(t *testing.T) {
	cases := []struct {
		ip   [4]byte
		want uint32
	}{
		{[4]byte{0, 0, 0, 0}, 0},
		{[4]byte{127, 0, 0, 1}, 0x0100007f}, // little-endian wire form
		{[4]byte{1, 2, 3, 4}, 0x04030201},
		{[4]byte{255, 255, 255, 255}, 0xffffffff},
	}
	for _, c := range cases {
		if got := ipBE(c.ip); got != c.want {
			t.Errorf("ipBE(%v) = %#x, want %#x", c.ip, got, c.want)
		}
	}
}

func TestPortBE(t *testing.T) {
	cases := []struct{ in, want uint16 }{
		{0, 0},
		{80, 0x5000},
		{9876, 0x9426},
		{0xff00, 0x00ff},
	}
	for _, c := range cases {
		if got := portBE(c.in); got != c.want {
			t.Errorf("portBE(%d) = %#x, want %#x", c.in, got, c.want)
		}
	}
}

func TestIsAgainAndInProgress(t *testing.T) {
	if !isAgain(-11) { // EAGAIN on linux
		t.Error("isAgain(-EAGAIN) should be true")
	}
	if isAgain(0) {
		t.Error("isAgain(0) should be false")
	}
	if !isInProgress(-115) { // EINPROGRESS on linux
		t.Error("isInProgress(-EINPROGRESS) should be true")
	}
	if isInProgress(-11) {
		t.Error("isInProgress(EAGAIN) should be false")
	}
}

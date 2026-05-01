package vclnet

import (
	"context"
	"net"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// addrFromInfo converts a vclpoll.AddrInfo to a *net.TCPAddr.
func addrFromInfo(info vclpoll.AddrInfo) *net.TCPAddr {
	var ip net.IP
	if info.IsV4 {
		ip = net.IP(info.IP[:4])
	} else {
		ip = net.IP(info.IP[:16])
	}
	return &net.TCPAddr{IP: ip, Port: int(info.Port)}
}

// udpAddrFromInfo converts a vclpoll.AddrInfo to a *net.UDPAddr.
func udpAddrFromInfo(info vclpoll.AddrInfo) *net.UDPAddr {
	var ip net.IP
	if info.IsV4 {
		ip = net.IP(info.IP[:4])
	} else {
		ip = net.IP(info.IP[:16])
	}
	return &net.UDPAddr{IP: ip, Port: int(info.Port)}
}

// parseNetwork validates the network string and returns whether it is
// IPv4-only, IPv6-only, or either.
func parseNetwork(network string) (ipv4Only, ipv6Only bool, err error) {
	switch network {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return false, false, net.UnknownNetworkError(network)
	}
	return network == "tcp4" || network == "udp4",
		network == "tcp6" || network == "udp6",
		nil
}

func isUDP(network string) bool {
	return network == "udp" || network == "udp4" || network == "udp6"
}

func networkBase(network string) string {
	if isUDP(network) {
		return "udp"
	}
	return "tcp"
}

func resolveAddr(network, address string) (*net.TCPAddr, error) {
	return resolveAddrContext(context.Background(), network, address)
}

// resolveAddrContext resolves one address. Unsuffixed networks prefer IPv4;
// the multi-address dial path uses resolveAddrs and Happy Eyeballs instead.
func resolveAddrContext(ctx context.Context, network, address string) (*net.TCPAddr, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := net.DefaultResolver.LookupPort(ctx, networkBase(network), portStr)
	if err != nil {
		return nil, err
	}

	if host == "" {
		return literalAddr("", port, network)
	}
	if ip := net.ParseIP(host); ip != nil {
		return literalAddr(host, port, network)
	}

	hostAddrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	ipv4Only := network == "tcp4" || network == "udp4"
	ipv6Only := network == "tcp6" || network == "udp6"

	var firstV6 net.IP
	for _, value := range hostAddrs {
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			if !ipv6Only {
				return &net.TCPAddr{IP: ip4, Port: port}, nil
			}
			continue
		}
		if !ipv4Only && firstV6 == nil {
			firstV6 = ip
		}
	}
	if firstV6 != nil {
		return &net.TCPAddr{IP: firstV6, Port: port}, nil
	}
	return nil, &net.DNSError{Err: "no suitable address found", Name: host}
}

// resolveAddrs resolves all TCP candidates used by Happy Eyeballs.
func resolveAddrs(ctx context.Context, network, address string) ([]*net.TCPAddr, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := net.DefaultResolver.LookupPort(ctx, "tcp", portStr)
	if err != nil {
		return nil, err
	}
	if host == "" || net.ParseIP(host) != nil {
		addr, err := literalAddr(host, port, network)
		if err != nil {
			return nil, err
		}
		return []*net.TCPAddr{addr}, nil
	}

	hostAddrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	ipv4Only := network == "tcp4"
	ipv6Only := network == "tcp6"

	results := make([]*net.TCPAddr, 0, len(hostAddrs))
	for _, value := range hostAddrs {
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			if !ipv6Only {
				results = append(results, &net.TCPAddr{IP: ip4, Port: port})
			}
		} else if !ipv4Only {
			results = append(results, &net.TCPAddr{IP: ip, Port: port})
		}
	}
	if len(results) == 0 {
		return nil, &net.DNSError{Err: "no suitable address found", Name: host}
	}
	return results, nil
}

func resolveUDPAddr(ctx context.Context, network, address string) (*net.UDPAddr, error) {
	tcpAddr, err := resolveAddrContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: tcpAddr.IP, Port: tcpAddr.Port}, nil
}

func resolveLiteral(host, portStr, network string) (*net.TCPAddr, error) {
	port, err := net.DefaultResolver.LookupPort(context.Background(), networkBase(network), portStr)
	if err != nil {
		return nil, err
	}
	return literalAddr(host, port, network)
}

func literalAddr(host string, port int, network string) (*net.TCPAddr, error) {
	if host == "" {
		if network == "tcp6" || network == "udp6" {
			return &net.TCPAddr{IP: net.IPv6zero, Port: port}, nil
		}
		return &net.TCPAddr{IP: net.IPv4zero, Port: port}, nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return nil, &net.DNSError{Err: "no such host", Name: host}
	}
	if ip4 := ip.To4(); ip4 != nil {
		if network == "tcp6" || network == "udp6" {
			return nil, &net.AddrError{Err: "IPv4 address not allowed", Addr: host}
		}
		return &net.TCPAddr{IP: ip4, Port: port}, nil
	}
	if network == "tcp4" || network == "udp4" {
		return nil, &net.AddrError{Err: "IPv6 address not allowed", Addr: host}
	}
	return &net.TCPAddr{IP: ip, Port: port}, nil
}

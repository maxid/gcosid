package gcosid

import (
	"fmt"
	"net"
)

// HostAddressSupplier reports this process's host address — CosID's
// HostAddressSupplier. It is used to build an InstanceId when the caller has no
// configured id.
type HostAddressSupplier interface {
	HostAddress() (string, error)
}

// LocalHostAddress picks a non-loopback, up interface address, falling back to
// the loopback address. CosID warns about multiple active interfaces; so does
// this implementation's documentation: pin the instance id when it matters.
type LocalHostAddress struct{}

// HostAddress implements HostAddressSupplier.
func (LocalHostAddress) HostAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("gcosid: list interfaces: %w", err)
	}
	var fallback string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.IsLoopback() || ipNet.IP.To4() == nil {
				continue
			}
			if fallback == "" {
				fallback = ipNet.IP.String()
			}
		}
	}
	if fallback != "" {
		return fallback, nil
	}

	// No usable interface address: report the loopback rather than guessing.
	return "127.0.0.1", nil
}

// InstanceIDFrom builds an instance id as `host:port` — InstanceId.of(host, port).
func InstanceIDFrom(supplier HostAddressSupplier, port int, stable bool) (InstanceID, error) {
	if supplier == nil {
		supplier = LocalHostAddress{}
	}
	host, err := supplier.HostAddress()
	if err != nil {
		return InstanceID{}, err
	}
	return InstanceID{ID: fmt.Sprintf("%s:%d", host, port), Stable: stable}, nil
}

package gcosid

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// DefaultHostnameEnv is the variable a Kubernetes StatefulSet pod gets: its own
// name, whose ordinal is the machine id (CosID's HOSTNAME_KEY).
const DefaultHostnameEnv = "HOSTNAME"

// StatefulSetDistributor takes the machine id from the pod ordinal instead of a
// lease — CosID's StatefulSetMachineIdDistributor. StatefulSet guarantees one pod
// per ordinal, which is exactly the guarantee a machine id needs; there is
// nothing to renew, so Guard and Revert are no-ops.
type StatefulSetDistributor struct {
	EnvKey   string
	MaxID    int64
	Lookup   func(string) (string, bool)
	resolved int64
}

// NewStatefulSetDistributor creates the distributor for a layout's machine range.
func NewStatefulSetDistributor(maxMachineID int64) (*StatefulSetDistributor, error) {
	if maxMachineID <= 0 {
		return nil, fmt.Errorf("%w: max machine id must be positive", ErrInvalidConfig)
	}
	return &StatefulSetDistributor{
		EnvKey:   DefaultHostnameEnv,
		MaxID:    maxMachineID,
		Lookup:   os.LookupEnv,
		resolved: -1,
	}, nil
}

// Distribute implements MachineIdDistributor.
func (d *StatefulSetDistributor) Distribute(context.Context, string) (int64, error) {
	if d.resolved >= 0 {
		return d.resolved, nil
	}

	id, err := d.ResolveMachineID()
	if err != nil {
		return 0, err
	}
	if id < 0 || id > d.MaxID {
		return 0, fmt.Errorf("%w: ordinal %d outside 0..%d", ErrInvalidMachineID, id, d.MaxID)
	}
	d.resolved = id
	return id, nil
}

// ResolveMachineID parses the pod ordinal from the hostname.
func (d *StatefulSetDistributor) ResolveMachineID() (int64, error) {
	key := d.EnvKey
	if key == "" {
		key = DefaultHostnameEnv
	}
	lookup := d.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}

	hostname, ok := lookup(key)
	if !ok || hostname == "" {
		return 0, fmt.Errorf("%w: %s is not set", ErrInvalidConfig, key)
	}
	// StatefulSet names are "<statefulset>-<ordinal>", so the ordinal follows the
	// last dash. A name without a dash is a configuration error, not id 0.
	idx := strings.LastIndex(hostname, "-")
	if idx <= 0 || idx == len(hostname)-1 {
		return 0, fmt.Errorf("%w: hostname %q has no ordinal", ErrInvalidConfig, hostname)
	}
	id, err := strconv.ParseInt(hostname[idx+1:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: hostname %q ordinal is not a number", ErrInvalidConfig, hostname)
	}
	return id, nil
}

// Revert implements MachineIdDistributor: the ordinal is owned by the scheduler.
func (d *StatefulSetDistributor) Revert(context.Context) error { return nil }

// Guard implements MachineIdDistributor: nothing to renew.
func (d *StatefulSetDistributor) Guard(context.Context, string) error { return nil }

// InstanceID implements MachineIdDistributor.
func (d *StatefulSetDistributor) InstanceID() string {
	key := d.EnvKey
	if key == "" {
		key = DefaultHostnameEnv
	}
	if lookup := d.Lookup; lookup != nil {
		if v, ok := lookup(key); ok {
			return v
		}
		return ""
	}
	return os.Getenv(key)
}

package gcosid

import (
	"context"
	"errors"
	"testing"
)

func hostnameLookup(value string) func(string) (string, bool) {
	return func(string) (string, bool) {
		if value == "" {
			return "", false
		}
		return value, true
	}
}

func TestStatefulSetResolvesTheOrdinal(t *testing.T) {
	cases := map[string]int64{
		"xdmp-iot-0": 0,
		"xdmp-iot-7": 7,
		"iot-12":     12,
	}
	for hostname, want := range cases {
		d, err := NewStatefulSetDistributor(1023)
		if err != nil {
			t.Fatalf("distributor: %v", err)
		}
		d.Lookup = hostnameLookup(hostname)

		got, err := d.Distribute(context.Background(), "order")
		if err != nil {
			t.Fatalf("%s: %v", hostname, err)
		}
		if got != want {
			t.Errorf("%s -> %d, want %d", hostname, got, want)
		}
		if d.InstanceID() != hostname {
			t.Errorf("instance id = %q, want %q", d.InstanceID(), hostname)
		}
	}
}

// A hostname without an ordinal is a configuration error, not machine id 0:
// silently defaulting would hand every such pod the same id.
func TestStatefulSetRejectsBadHostnames(t *testing.T) {
	for _, hostname := range []string{"", "nodash", "-5", "xdmp-iot-", "xdmp-iot-abc"} {
		d, _ := NewStatefulSetDistributor(1023)
		d.Lookup = hostnameLookup(hostname)
		if _, err := d.Distribute(context.Background(), "order"); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%q err = %v, want ErrInvalidConfig", hostname, err)
		}
	}
}

func TestStatefulSetRejectsOrdinalOutsideLayout(t *testing.T) {
	d, _ := NewStatefulSetDistributor(3)
	d.Lookup = hostnameLookup("xdmp-iot-4")
	if _, err := d.Distribute(context.Background(), "order"); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("err = %v, want ErrInvalidMachineID", err)
	}
}

func TestStatefulSetGuardAndRevertAreNoops(t *testing.T) {
	d, _ := NewStatefulSetDistributor(10)
	d.Lookup = hostnameLookup("xdmp-iot-1")

	if err := d.Guard(context.Background(), "order"); err != nil {
		t.Fatalf("guard: %v", err)
	}
	if err := d.Revert(context.Background()); err != nil {
		t.Fatalf("revert: %v", err)
	}
}

func TestNewStatefulSetDistributorValidates(t *testing.T) {
	if _, err := NewStatefulSetDistributor(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero max err = %v", err)
	}
}

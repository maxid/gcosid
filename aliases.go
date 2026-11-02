package gcosid

import (
	"github.com/maxid/gcosid/core"
	"github.com/maxid/gcosid/machine"
)

// The vocabulary every generator, converter and allocator shares lives in ./core: the
// generator contracts, the segment model, the radix/prefix vocabulary, the snowflake
// layout and the Stat tree. Every name below is an alias, so a consumer keeps importing
// gcosid and still gets the core type — one definition, two import paths.
type (
	// core vocabulary
	Generator         = core.Generator
	NamedGenerator    = core.NamedGenerator
	StringConverter   = core.StringConverter
	Segment           = core.Segment
	SegmentAllocator  = core.SegmentAllocator
	Radix             = core.Radix
	Prefix            = core.Prefix
	Suffix            = core.Suffix
	DatePrefix        = core.DatePrefix
	GroupedPrefix     = core.GroupedPrefix
	ToString          = core.ToString
	SnowflakeFriendly = core.SnowflakeFriendly
	Layout            = core.Layout
	State             = core.State
	Stat              = core.Stat
	Statistical       = core.Statistical

	// machine-id distribution
	InstanceID             = machine.InstanceID
	NamespacedInstanceID   = machine.NamespacedInstanceID
	GuardianState          = machine.GuardianState
	Guarder                = machine.Guarder
	GuarderOption          = machine.GuarderOption
	NoopGuarder            = machine.NoopGuarder
	HostAddressSupplier    = machine.HostAddressSupplier
	LocalHostAddress       = machine.LocalHostAddress
	LocalStateStorage      = machine.LocalStateStorage
	MachineState           = machine.MachineState
	ManualDistributor      = machine.ManualDistributor
	MachineStateStorage    = machine.MachineStateStorage
	MachineIdDistributor   = machine.MachineIdDistributor
	MemoryMachineStorage   = machine.MemoryMachineStorage
	LeaseDistributor       = machine.LeaseDistributor
	LeaseOption            = machine.LeaseOption
	StatefulSetDistributor = machine.StatefulSetDistributor
)

const (
	TTLForever = core.TTLForever

	CosIdEpoch         = core.CosIdEpoch
	CosIdEpochSecond   = core.CosIdEpochSecond
	TotalBit           = core.TotalBit
	JSMaxSafeNumberBit = core.JSMaxSafeNumberBit
	JSMaxSafeNumber    = core.JSMaxSafeNumber

	DefaultHostnameEnv = machine.DefaultHostnameEnv
)

var (
	MillisecondLayout       = core.MillisecondLayout
	SecondLayout            = core.SecondLayout
	SafeJSMillisecondLayout = core.SafeJSMillisecondLayout
	SafeJSSecondLayout      = core.SafeJSSecondLayout

	ErrClosed               = core.ErrClosed
	ErrAllocatorUnavailable = core.ErrAllocatorUnavailable
	ErrInvalidConfig        = core.ErrInvalidConfig
	ErrExhausted            = core.ErrExhausted
	ErrClockMovedBackwards  = core.ErrClockMovedBackwards
	ErrInvalidMachineID     = core.ErrInvalidMachineID
	ErrNamespaceConflict    = core.ErrNamespaceConflict
)

// Constructors and options keep their names by forwarding to the package that owns them.
var (
	NewStat  = core.NewStat
	NewRadix = core.NewRadix

	NewInstanceID             = machine.NewInstanceID
	WithGuardTiming           = machine.WithGuardTiming
	WithGuardClock            = machine.WithGuardClock
	NewGuarder                = machine.NewGuarder
	InstanceIDFrom            = machine.InstanceIDFrom
	DefaultStatePath          = machine.DefaultStatePath
	NewLocalStateStorage      = machine.NewLocalStateStorage
	NewMemoryMachineStorage   = machine.NewMemoryMachineStorage
	WithLeaseStable           = machine.WithLeaseStable
	WithLeaseSafeGuard        = machine.WithLeaseSafeGuard
	NewLeaseDistributor       = machine.NewLeaseDistributor
	NewStatefulSetDistributor = machine.NewStatefulSetDistributor
)

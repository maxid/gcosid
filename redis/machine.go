package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/maxid/gcosid"
)

// The scripts below are CosID 2.15.1's machine_id_distribute / _guard / _revert
// verbatim. They keep the same keys (`cosid:<namespace>:itc_idx`, `:adder`,
// `:revert`) and the same value format (`<machineId>|<lastStamp>`), so a Go
// process and a CosID process share one slot table.
const distributeScript = `
local stateDelimiter = "|";
local keyPrefix = 'cosid' .. ':';
local namespace = KEYS[1];
local instanceId = ARGV[1];
local maxMachineId = tonumber(ARGV[2]);
local currentStamp = tonumber(ARGV[3]);
local safeGuardAt = tonumber(ARGV[4]);

local instanceIdxKey = keyPrefix .. namespace .. ':itc_idx';
local machineIdAdderKey = keyPrefix .. namespace .. ':adder';
local instanceRevertKey = keyPrefix .. namespace .. ':revert';

local function convertStingToState(machineState)
    local splitIdx = string.find(machineState, stateDelimiter, 1);
    local machineId = string.sub(machineState, 1, splitIdx - 1);
    local timestamp = string.sub(machineState, splitIdx + 1, -1);
    return { tonumber(machineId), tonumber(timestamp) }
end

local function convertStateToString(machineId, timestamp)
    return tostring(machineId) .. stateDelimiter .. tostring(timestamp);
end

local function setState(machineId, lastStamp)
    local machineState = convertStateToString(machineId, lastStamp);
    redis.call('hset', instanceIdxKey, instanceId, machineState);
end

local function moveRevertToCurrent(machineId, lastStamp)
    if lastStamp < currentStamp then
        lastStamp = currentStamp;
    end
    setState(machineId, lastStamp);
    redis.call('hdel', instanceRevertKey, machineId)
    return { machineId, lastStamp }
end

local function moveRecyclableToCurrent(recyclableInstanceId, machineId)
    redis.call('hdel', instanceIdxKey, recyclableInstanceId);
    setState(machineId, currentStamp);
    return { machineId, currentStamp }
end

local machineState = redis.call('hget', instanceIdxKey, instanceId)
if machineState then
    local states = convertStingToState(machineState);
    local machineId = states[1];
    setState(machineId, currentStamp);
    return { machineId, currentStamp }
end

local machineData = redis.call('hgetall', instanceRevertKey);
if #machineData > 0 then
    local machineId = tonumber(machineData[1]);
    local lastStamp = tonumber(machineData[2]);
    return moveRevertToCurrent(machineId, lastStamp)
end

local lastMachineId = redis.call('get', machineIdAdderKey)
if not lastMachineId then
    lastMachineId = -1;
    redis.call('set', machineIdAdderKey, lastMachineId)
end

lastMachineId = tonumber(lastMachineId);
if lastMachineId < maxMachineId then
    local machineId = redis.call('incr', machineIdAdderKey);
    setState(machineId, currentStamp);
    return { machineId, currentStamp };
end

local instanceData = redis.call('hgetall', instanceIdxKey);

for idx, val in ipairs(instanceData) do
    if idx % 2 == 1 then
        local eachInstanceId = val;
        local eachMachineStateStr = instanceData[idx + 1];
        local eachMachineState = convertStingToState(eachMachineStateStr);
        local eachMachineId = eachMachineState[1];
        local eachLastStamp = eachMachineState[2];
        if eachLastStamp <= safeGuardAt then
            return moveRecyclableToCurrent(eachInstanceId, eachMachineId);
        end
    end
end

return { -1, -1 };
`

const guardScript = `
local stateDelimiter = "|";
local namespace = KEYS[1];
local instanceId = ARGV[1];
local currentStamp = ARGV[2];
local instanceIdxKey = 'cosid' .. ':' .. namespace .. ':itc_idx';

local function convertStingToState(machineState)
    local splitIdx = string.find(machineState, stateDelimiter, 1);
    local machineId = string.sub(machineState, 1, splitIdx - 1);
    local timestamp = string.sub(machineState, splitIdx + 1, -1);
    return { tonumber(machineId), tonumber(timestamp) }
end

local function convertStateToString(machineId, timestamp)
    return tostring(machineId) .. stateDelimiter .. tostring(timestamp);
end

local function setState(machineId, lastStamp)
    local machineState = convertStateToString(machineId, lastStamp);
    redis.call('hset', instanceIdxKey, instanceId, machineState);
end

local machineState = redis.call('hget', instanceIdxKey, instanceId)
if machineState then
    local states = convertStingToState(machineState)
    local machineId = states[1];
    setState(machineId, currentStamp);
    return 1;
end

return 0;
`

const revertScript = `
local stateDelimiter = "|";
local namespace = KEYS[1];
local instanceId = ARGV[1];
local currentStamp = ARGV[2];
local instanceIdxKey = 'cosid' .. ':' .. namespace .. ':itc_idx';
local instanceRevertKey = 'cosid' .. ':' .. namespace .. ':revert';

local function convertStingToState(machineState)
    local splitIdx = string.find(machineState, stateDelimiter, 1);
    local machineId = string.sub(machineState, 1, splitIdx - 1);
    local timestamp = string.sub(machineState, splitIdx + 1, -1);
    return { tonumber(machineId), tonumber(timestamp) }
end

local machineState = redis.call('hget', instanceIdxKey, instanceId)
if machineState then
    redis.call('hdel', instanceIdxKey, instanceId);
    local states = convertStingToState(machineState);
    local machineId = states[1];
    redis.call('hset', instanceRevertKey, machineId, currentStamp);
    return 1;
end

return 0;
`

// MachineStorage implements gcosid.MachineStateStorage with CosID's three Lua
// scripts. The keys, the value format and the safe-guard predicate are the Java
// ones, so a Go process and a Java process share one slot table.
type MachineStorage struct {
	client    Client
	namespace string
	safeGuard time.Duration
	now       func() int64
}

// MachineOption configures MachineStorage.
type MachineOption func(*MachineStorage)

// WithSafeGuard sets how long a slot stays reserved after its last heartbeat.
func WithSafeGuard(d time.Duration) MachineOption {
	return func(m *MachineStorage) {
		if d > 0 {
			m.safeGuard = d
		}
	}
}

// WithClock overrides the timestamp source (tests).
func WithClock(now func() int64) MachineOption {
	return func(m *MachineStorage) {
		if now != nil {
			m.now = now
		}
	}
}

// NewMachineStorage builds the machine slot store for a namespace.
func NewMachineStorage(client Client, namespace string, opts ...MachineOption) (*MachineStorage, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gcosid.ErrInvalidConfig)
	}
	if namespace == "" {
		return nil, fmt.Errorf("%w: namespace is required", gcosid.ErrInvalidConfig)
	}
	m := &MachineStorage{
		client:    client,
		namespace: namespace,
		safeGuard: 10 * time.Minute,
		now:       func() int64 { return time.Now().UnixMilli() },
	}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Distribute implements gcosid.MachineStateStorage.
func (m *MachineStorage) Distribute(ctx context.Context, namespace, instanceID string, maxMachineID int64, safeGuard time.Duration, stable bool) (gcosid.MachineState, error) {
	if namespace == "" {
		namespace = m.namespace
	}
	now := m.now()
	reply, err := m.client.Eval(ctx, distributeScript, []string{namespace},
		instanceID, maxMachineID, now, gcosid.SafeGuardAt(now, m.effectiveSafeGuard(safeGuard), stable))
	if err != nil {
		return gcosid.MachineState{}, fmt.Errorf("%w: machine distribute: %w", gcosid.ErrAllocatorUnavailable, err)
	}

	machineID, lastStamp, err := parseState(reply)
	if err != nil {
		return gcosid.MachineState{}, err
	}
	if machineID < 0 {
		return gcosid.MachineState{}, fmt.Errorf("%w: namespace %s exhausted", gcosid.ErrExhausted, namespace)
	}
	return gcosid.MachineState{MachineID: machineID, LastTimestamp: lastStamp, InstanceID: instanceID}, nil
}

// Guard implements gcosid.MachineStateStorage (the guard script).
func (m *MachineStorage) Guard(ctx context.Context, namespace string, state gcosid.MachineState, _ time.Duration, _ bool) error {
	if namespace == "" {
		namespace = m.namespace
	}
	reply, err := m.client.Eval(ctx, guardScript, []string{namespace}, state.InstanceID, state.LastTimestamp)
	if err != nil {
		return fmt.Errorf("gcosid/redis: machine guard: %w", err)
	}
	if asInt(reply) == 0 {
		return fmt.Errorf("%w: no slot for instance %s", gcosid.ErrInvalidConfig, state.InstanceID)
	}
	return nil
}

// Revert implements gcosid.MachineStateStorage (the revert script). The script
// keeps the instance id and moves the slot to the revert hash, which is CosID's
// behaviour for both stable and unstable instances on Redis.
func (m *MachineStorage) Revert(ctx context.Context, namespace, instanceID string, _ gcosid.MachineState, _ bool) error {
	if namespace == "" {
		namespace = m.namespace
	}
	if _, err := m.client.Eval(ctx, revertScript, []string{namespace}, instanceID, m.now()); err != nil {
		return fmt.Errorf("gcosid/redis: machine revert: %w", err)
	}
	return nil
}

// effectiveSafeGuard prefers the caller's window and falls back to the storage's.
func (m *MachineStorage) effectiveSafeGuard(safeGuard time.Duration) time.Duration {
	if safeGuard > 0 {
		return safeGuard
	}
	return m.safeGuard
}

func parseState(reply any) (int64, int64, error) {
	values, ok := reply.([]any)
	if !ok || len(values) < 2 {
		return 0, 0, fmt.Errorf("%w: unexpected machine reply %T", gcosid.ErrInvalidConfig, reply)
	}
	return asInt(values[0]), asInt(values[1]), nil
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		var out int64
		_, _ = fmt.Sscanf(n, "%d", &out)
		return out
	default:
		return -1
	}
}

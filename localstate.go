package gcosid

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LocalStateStorage caches the machine state on disk — CosID's
// LocalMachineStateStorage (`~/.cosid-machine-state`). The remote store stays the
// source of truth; the file lets a restart reuse its machine id without a remote
// round trip, and survives the remote being briefly unavailable.
type LocalStateStorage struct {
	Path string

	mu    sync.Mutex
	state map[string]MachineState // key: namespace|instance
}

// DefaultStatePath is CosID's DEFAULT_STATE_LOCATION_PATH.
func DefaultStatePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".cosid-machine-state")
}

// NewLocalStateStorage loads (or starts) a file-backed cache.
func NewLocalStateStorage(path string) (*LocalStateStorage, error) {
	if path == "" {
		path = DefaultStatePath()
	}
	s := &LocalStateStorage{Path: path, state: map[string]MachineState{}}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("gcosid: read local state: %w", err)
	}
	if len(raw) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(raw, &s.state); err != nil {
		// A corrupt cache is not fatal: the remote store re-issues the slot.
		s.state = map[string]MachineState{}
	}
	return s, nil
}

// Get returns the cached state of (namespace, instance).
func (l *LocalStateStorage) Get(namespace, instanceID string) (MachineState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, ok := l.state[keyOf(namespace, instanceID)]
	return state, ok
}

// Set records the state and persists it.
func (l *LocalStateStorage) Set(namespace, instanceID string, state MachineState) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	state.InstanceID = instanceID
	l.state[keyOf(namespace, instanceID)] = state
	raw, err := json.Marshal(l.state)
	if err != nil {
		return fmt.Errorf("gcosid: encode local state: %w", err)
	}
	if err := os.WriteFile(l.Path, raw, 0o600); err != nil {
		return fmt.Errorf("gcosid: write local state: %w", err)
	}
	return nil
}

// Remove drops one entry and persists the rest.
func (l *LocalStateStorage) Remove(namespace, instanceID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.state, keyOf(namespace, instanceID))
	raw, err := json.Marshal(l.state)
	if err != nil {
		return fmt.Errorf("gcosid: encode local state: %w", err)
	}
	if err := os.WriteFile(l.Path, raw, 0o600); err != nil {
		return fmt.Errorf("gcosid: write local state: %w", err)
	}
	return nil
}

func keyOf(namespace, instanceID string) string { return namespace + "|" + instanceID }

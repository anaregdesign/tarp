package qualification

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type OracleEvent struct {
	Type      string           `json:"type"`
	Key       []byte           `json:"key,omitempty"`
	Value     []byte           `json:"value,omitempty"`
	Phase     string           `json:"phase,omitempty"`
	Sequence  uint64           `json:"sequence,omitempty"`
	Count     uint64           `json:"count,omitempty"`
	Mode      string           `json:"mode,omitempty"`
	Mutations []OracleMutation `json:"mutations,omitempty"`
}

type OracleMutation struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

func ParseOracle(line string) (OracleEvent, bool, error) {
	line, recognized := ConsolePayload(line)
	if !recognized {
		return OracleEvent{}, false, nil
	}
	parts := strings.Fields(line)
	if len(parts) == 2 && (parts[0] == "TARP_BATCH_INTENT" || parts[0] == "TARP_BATCH_ACK" || parts[0] == "TARP_BATCH_NOT_APPLIED") {
		data, err := hex.DecodeString(parts[1])
		if err != nil {
			return OracleEvent{}, false, err
		}
		var mutations []OracleMutation
		if err := json.Unmarshal(data, &mutations); err != nil {
			return OracleEvent{}, false, err
		}
		return OracleEvent{Type: strings.ToLower(strings.TrimPrefix(parts[0], "TARP_")), Mutations: mutations}, true, nil
	}
	if len(parts) == 2 && parts[0] == "TARP_PHASE" {
		return OracleEvent{Type: "phase", Phase: parts[1]}, true, nil
	}
	if len(parts) == 2 {
		switch parts[0] {
		case "TARP_CAPTURE", "TARP_READS", "TARP_PINNED", "TARP_REUSED":
			value, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return OracleEvent{}, false, err
			}
			if parts[0] == "TARP_CAPTURE" {
				return OracleEvent{Type: "capture", Sequence: value}, true, nil
			}
			return OracleEvent{Type: strings.ToLower(strings.TrimPrefix(parts[0], "TARP_")), Count: value}, true, nil
		case "TARP_OPERATION_ACK":
			return OracleEvent{Type: "operation_ack", Mode: parts[1]}, true, nil
		}
	}
	kind := map[string]string{
		"TARP_ACK": "ack", "TARP_INTENT": "intent",
		"TARP_ACK_DELETE": "ack_delete", "TARP_INTENT_DELETE": "intent_delete",
		"TARP_NOT_APPLIED": "not_applied", "TARP_ERROR_COMMITTED": "error_committed",
	}[parts[0]]
	deleting := kind == "ack_delete" || kind == "intent_delete"
	keyOnly := deleting || kind == "not_applied" || kind == "error_committed"
	if kind == "" || keyOnly && len(parts) != 2 || !keyOnly && len(parts) != 3 {
		return OracleEvent{}, false, fmt.Errorf("invalid guest oracle event: %q", line)
	}
	key, err := hex.DecodeString(parts[1])
	if err != nil || len(key) == 0 {
		return OracleEvent{}, false, errors.New("oracle keys must be nonempty hexadecimal")
	}
	value := []byte{}
	if !keyOnly && parts[2] != "-" {
		value, err = hex.DecodeString(parts[2])
		if err != nil {
			return OracleEvent{}, false, err
		}
	}
	return OracleEvent{Type: kind, Key: key, Value: value}, true, nil
}

func ReadOracle(input io.Reader) ([]OracleEvent, error) {
	reader := bufio.NewReaderSize(io.LimitReader(input, (8<<20)+1), 1<<20)
	var events []OracleEvent
	total := 0
	for {
		line, err := reader.ReadSlice('\n')
		if err == io.EOF && len(line) == 0 {
			return events, nil
		}
		if err != nil {
			return nil, fmt.Errorf("incomplete or oversized oracle line: %w", err)
		}
		total += len(line)
		if total > 8<<20 || len(events) == 8192 {
			return nil, errors.New("qualification: oracle history exceeds its bound")
		}
		var event OracleEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
}

type oracleValue struct {
	Key     []byte `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Present bool   `json:"present"`
}

type Oracle struct {
	values  map[string]oracleValue
	pending *OracleEvent
	Phase   string
	acks    uint64
}

func ReplayOracle(events []OracleEvent, allowEmptyInitialization bool) (*Oracle, error) {
	oracle := &Oracle{values: make(map[string]oracleValue)}
	for _, event := range events {
		if oracle.Phase != "" {
			return nil, errors.New("oracle events after the terminal phase barrier")
		}
		deleting := event.Type == "ack_delete" || event.Type == "intent_delete"
		switch event.Type {
		case "batch_intent", "batch_ack", "batch_not_applied":
			if _, err := batchMutations(event); err != nil {
				return nil, err
			}
			if event.Type == "batch_intent" {
				if oracle.pending != nil {
					return nil, errors.New("batch overlaps another pending mutation")
				}
				pending := event
				pending.Mutations = make([]OracleMutation, len(event.Mutations))
				for i, m := range event.Mutations {
					pending.Mutations[i] = OracleMutation{Key: bytes.Clone(m.Key), Value: bytes.Clone(m.Value), Delete: m.Delete}
				}
				oracle.pending = &pending
				break
			}
			pending := oracle.pending
			if pending == nil || pending.Type != "batch_intent" || !reflect.DeepEqual(pending.Mutations, event.Mutations) {
				return nil, errors.New("batch outcome differs from pending batch")
			}
			mutations, _ := batchMutations(*pending)
			for _, m := range mutations {
				key := string(m.Key)
				if event.Type == "batch_ack" {
					oracle.values[key] = oracleValue{Key: m.Key, Value: m.Value, Present: !m.Delete}
					oracle.acks++
				} else if _, exists := oracle.values[key]; !exists {
					oracle.values[key] = oracleValue{Key: m.Key}
				}
			}
			oracle.pending = nil
		case "ack", "ack_delete":
			if len(event.Key) == 0 || deleting && len(event.Value) != 0 {
				return nil, errors.New("invalid acknowledged mutation")
			}
			if pending := oracle.pending; pending != nil {
				want := "intent"
				if deleting {
					want = "intent_delete"
				}
				if pending.Type != want || !bytes.Equal(pending.Key, event.Key) || !bytes.Equal(pending.Value, event.Value) {
					return nil, errors.New("ack does not match pending intent")
				}
				oracle.pending = nil
			}
			oracle.values[string(event.Key)] = oracleValue{
				Key: bytes.Clone(event.Key), Value: bytes.Clone(event.Value), Present: !deleting,
			}
			oracle.acks++
		case "intent", "intent_delete":
			if oracle.pending != nil || len(event.Key) == 0 || deleting && len(event.Value) != 0 {
				return nil, errors.New("one nonempty-key pending mutation at a time is required")
			}
			pending := event
			pending.Key, pending.Value = bytes.Clone(event.Key), bytes.Clone(event.Value)
			oracle.pending = &pending
		case "not_applied", "error_committed":
			pending := oracle.pending
			if pending == nil || !bytes.Equal(pending.Key, event.Key) || len(event.Value) != 0 {
				return nil, errors.New("definitive outcome does not match a pending mutation")
			}
			if event.Type == "error_committed" {
				oracle.values[string(event.Key)] = oracleValue{
					Key: pending.Key, Value: pending.Value, Present: pending.Type == "intent",
				}
				oracle.acks++
			} else if _, exists := oracle.values[string(event.Key)]; !exists {
				oracle.values[string(event.Key)] = oracleValue{Key: pending.Key}
			}
			oracle.pending = nil
		case "phase":
			if event.Phase == "" {
				return nil, errors.New("empty phase barrier")
			}
			oracle.Phase = event.Phase
		case "capture":
			if oracle.pending != nil || event.Sequence != oracle.acks {
				return nil, errors.New("capture sequence does not match the complete acknowledged prefix")
			}
		case "reads", "pinned", "reused":
		case "operation_ack":
			if event.Mode == "" || oracle.pending != nil {
				return nil, errors.New("invalid operation acknowledgment")
			}
		default:
			return nil, fmt.Errorf("unknown oracle event: %q", event.Type)
		}
	}
	if oracle.Phase == "" || !allowEmptyInitialization && oracle.acks == 0 {
		return nil, errors.New("a durably observed acknowledgment and terminal phase barrier are required")
	}
	return oracle, nil
}

func batchMutations(event OracleEvent) ([]OracleMutation, error) {
	if len(event.Mutations) == 0 || len(event.Mutations) > 2048 || len(event.Key) != 0 || len(event.Value) != 0 {
		return nil, errors.New("invalid bounded batch witness")
	}
	deleting := event.Mutations[0].Delete
	seen := make(map[string]bool)
	var effective []OracleMutation
	for _, m := range event.Mutations {
		if len(m.Key) == 0 || m.Delete != deleting || m.Delete && len(m.Value) != 0 {
			return nil, errors.New("invalid or mixed batch mutation")
		}
		if !deleting || !seen[string(m.Key)] {
			effective = append(effective, m)
		}
		seen[string(m.Key)] = true
	}
	return effective, nil
}

func (o *Oracle) pendingMutations() []OracleMutation {
	if o.pending == nil {
		return nil
	}
	if o.pending.Type == "batch_intent" {
		mutations, _ := batchMutations(*o.pending)
		return mutations
	}
	return []OracleMutation{{Key: o.pending.Key, Value: o.pending.Value, Delete: o.pending.Type == "intent_delete"}}
}

func (o *Oracle) SequenceRange() (uint64, uint64) {
	upper := o.acks + uint64(len(o.pendingMutations()))
	return o.acks, upper
}

// PowerCutSequenceRange constrains a pending mutation at its own publication
// barrier. In other campaign modes a barrier can belong to maintenance.
func (o *Oracle) PowerCutSequenceRange(mode string) (uint64, uint64) {
	low, high := o.SequenceRange()
	switch mode {
	case "put", "delete", "insert", "reinsert", "rotate", "batch-put", "batch-delete", "batch-rotate", "pager-exhaustion":
		switch o.Phase {
		case "data.write:before", "data.write:after", "data.sync:before", "data.sync:after",
			"segment.write:before", "segment.write:after",
			"segment.sync:before", "segment.sync:after", "catalog.write:before", "catalog.write:after",
			"catalog.sync:before", "catalog.sync:after", "catalog.dirsync:before", "catalog.dirsync:after",
			"current.write:before", "current.write:after", "current.sync:before", "current.sync:after",
			"current.rename:before":
			return low, low
		case "current.dirsync:after":
			return high, high
		}
	}
	return low, high
}

// StateAt binds the one pending mutation to the actual reopened frontier.
// The complete returned live set also makes unexpected index entries detectable.
func (o *Oracle) StateAt(sequence uint64) (map[string][]byte, error) {
	low, high := o.SequenceRange()
	if sequence != low && sequence != high {
		return nil, fmt.Errorf("recovered sequence %d outside [%d,%d]", sequence, low, high)
	}
	state := make(map[string][]byte, len(o.values)+1)
	for key, value := range o.values {
		if value.Present {
			state[key] = bytes.Clone(value.Value)
		}
	}
	if sequence == high {
		for _, pending := range o.pendingMutations() {
			key := string(pending.Key)
			if pending.Delete {
				if _, present := state[key]; !present {
					return nil, errors.New("an absent-key Delete cannot advance the committed sequence")
				}
				delete(state, key)
			} else {
				state[key] = bytes.Clone(pending.Value)
			}
		}
	}
	return state, nil
}

func (o *Oracle) VerifyAt(sequence uint64, liveKeys int, lookup func([]byte) ([]byte, bool, error)) (string, error) {
	state, err := o.StateAt(sequence)
	if err != nil {
		return "", err
	}
	if liveKeys != len(state) {
		return "", fmt.Errorf("recovered live index cardinality %d != witnessed %d", liveKeys, len(state))
	}
	return o.Verify(func(key []byte) ([]byte, bool, error) {
		actual, present, err := lookup(key)
		expected, expectedPresent := state[string(key)]
		if err == nil && (present != expectedPresent || present && !bytes.Equal(actual, expected)) {
			return nil, false, fmt.Errorf("key %x disagrees with reopened sequence %d", key, sequence)
		}
		return actual, present, err
	})
}

// DigestAt permits the external witness to independently reconcile a recorded
// recovery digest, without reading or repairing the guest's data files.
func (o *Oracle) DigestAt(sequence uint64) (string, int, error) {
	state, err := o.StateAt(sequence)
	if err != nil {
		return "", 0, err
	}
	digest, err := o.VerifyAt(sequence, len(state), func(key []byte) ([]byte, bool, error) {
		value, present := state[string(key)]
		return value, present, nil
	})
	return digest, len(state), err
}

// Verify runs only after file-only recovery; the oracle never repairs files.
// A pending mutation may be entirely absent or applied, never a partial value.
func (o *Oracle) Verify(lookup func([]byte) ([]byte, bool, error)) (string, error) {
	keys := make([]string, 0, len(o.values)+1)
	for key := range o.values {
		keys = append(keys, key)
	}
	seen := make(map[string]bool)
	for _, key := range keys {
		seen[key] = true
	}
	for _, pending := range o.pendingMutations() {
		if !seen[string(pending.Key)] {
			keys = append(keys, string(pending.Key))
			seen[string(pending.Key)] = true
		}
	}
	sort.Strings(keys)
	low, high := o.SequenceRange()
	oldState, err := o.StateAt(low)
	if err != nil {
		return "", err
	}
	newState, err := o.StateAt(high)
	if err != nil {
		return "", err
	}
	oldAllowed, newAllowed := true, true
	actual := make([]oracleValue, 0, len(keys))
	for _, key := range keys {
		value, present, err := lookup([]byte(key))
		if err != nil {
			return "", fmt.Errorf("oracle read %x: %w", key, err)
		}
		oldValue, oldPresent := oldState[key]
		newValue, newPresent := newState[key]
		oldAllowed = oldAllowed && present == oldPresent && (!present || bytes.Equal(value, oldValue))
		newAllowed = newAllowed && present == newPresent && (!present || bytes.Equal(value, newValue))
		if !oldAllowed && !newAllowed {
			return "", fmt.Errorf("acknowledged/pending key %x has a forbidden recovered state: present=%t value=%x", key, present, value)
		}
		if !present {
			value = nil
		}
		actual = append(actual, oracleValue{Key: []byte(key), Value: value, Present: present})
	}
	data, err := json.Marshal(actual)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

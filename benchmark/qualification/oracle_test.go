package qualification

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestBatchOracleRejectsPartialSequencesAndMixedStates(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		mutations := []OracleMutation{
			{Key: []byte("a"), Value: []byte("first"), Delete: deleting},
			{Key: []byte("b"), Value: []byte("second"), Delete: deleting},
			{Key: []byte("a"), Value: []byte("last"), Delete: deleting},
		}
		if deleting {
			for i := range mutations {
				mutations[i].Value = nil
			}
		}
		data, err := json.Marshal(mutations)
		if err != nil {
			t.Fatal(err)
		}
		intent, recognized, err := ParseOracle("TARP_BATCH_INTENT " + hex.EncodeToString(data))
		if err != nil || !recognized {
			t.Fatalf("batch protocol: %v", err)
		}
		events := []OracleEvent{
			{Type: "ack", Key: []byte("a"), Value: []byte("old")},
			{Type: "ack", Key: []byte("b"), Value: []byte("old")},
			intent, {Type: "phase", Phase: "current.rename:after"},
		}
		oracle, err := ReplayOracle(events, false)
		if err != nil {
			t.Fatal(err)
		}
		lo, hi := oracle.SequenceRange()
		want := uint64(5)
		if deleting {
			want = 4 // Duplicate deletes have one effective record per key.
		}
		if lo != 2 || hi != want {
			t.Fatalf("wrong atomic batch endpoints [%d,%d]", lo, hi)
		}
		for seq := lo + 1; seq < hi; seq++ {
			if _, err := oracle.StateAt(seq); err == nil {
				t.Fatal("accepted a partial batch sequence")
			}
			for _, sequence := range []uint64{lo, hi} {
				state, err := oracle.StateAt(sequence)
				if err != nil {
					t.Fatal(err)
				}
				lookup := func(key []byte) ([]byte, bool, error) {
					value, present := state[string(key)]
					return value, present, nil
				}
				digest, err := oracle.VerifyAt(sequence, len(state), lookup)
				want, count, digestErr := oracle.DigestAt(sequence)
				if err != nil || digestErr != nil || digest != want || count != len(state) {
					t.Fatalf("batch endpoint verification: %v %v", err, digestErr)
				}
			}
		}
		_, err = oracle.Verify(func(key []byte) ([]byte, bool, error) {
			if bytes.Equal(key, []byte("b")) {
				return []byte("old"), true, nil
			}
			return []byte("last"), !deleting, nil
		})
		if err == nil {
			t.Fatal("accepted mixed pre/post batch state")
		}
		for _, kind := range []string{"batch_ack", "batch_not_applied"} {
			outcome := OracleEvent{Type: kind, Mutations: mutations}
			events = []OracleEvent{events[0], events[1], intent, outcome, {Type: "phase", Phase: "complete"}}
			got, err := ReplayOracle(events, false)
			if err != nil {
				t.Fatal(err)
			}
			want := lo
			if kind == "batch_ack" {
				want = hi
			}
			if low, high := got.SequenceRange(); low != want || high != want {
				t.Fatal("definitive batch outcome retained an uncertainty window")
			}
			outcome.Mutations = append([]OracleMutation(nil), mutations...)
			outcome.Mutations[0].Key = []byte("different")
			events[3] = outcome
			if _, err := ReplayOracle(events, false); err == nil {
				t.Fatal("accepted an outcome for a different batch")
			}
		}
	}
}

func TestExtendedCampaignGroupsAreBoundedAndDisjoint(t *testing.T) {
	seen := make(map[string]bool)
	var count int
	for _, group := range []string{"initialize", "mutate", "checkpoint", "snapshots", "recovery", "exhaustion",
		"batches", "cleaner", "ordered", "indexes", "repair", "pager"} {
		cases, err := CampaignCases(group)
		if err != nil || len(cases) == 0 || len(cases) > 192 {
			t.Fatalf("unbounded/empty campaign group %q: %d %v", group, len(cases), err)
		}
		for _, cfg := range cases {
			if seen[cfg.Name] {
				t.Fatalf("duplicate externally addressed case %q", cfg.Name)
			}
			seen[cfg.Name] = true
			count++
		}
	}
	all, err := CampaignCases("all")
	if err != nil || len(all) != count {
		t.Fatalf("catalog omitted cases: %d != %d, %v", len(all), count, err)
	}
}

func TestDeleteAndReinsertOracleNeverAllowsResurrection(t *testing.T) {
	cases := []struct {
		history string
		allowed []string
	}{
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT_DELETE 6b\n", []string{"old", "<missing>"}},
		{"TARP_ACK 6b 6f6c64\nTARP_ACK_DELETE 6b\n", []string{"<missing>"}},
		{"TARP_ACK 6b 6f6c64\nTARP_ACK_DELETE 6b\nTARP_INTENT 6b 6e6577\n", []string{"<missing>", "new"}},
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT_DELETE 6b\nTARP_ACK_DELETE 6b\nTARP_INTENT 6b 6e6577\nTARP_ACK 6b 6e6577\n", []string{"new"}},
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT 6b 6e6577\nTARP_NOT_APPLIED 6b\n", []string{"old"}},
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT_DELETE 6b\nTARP_NOT_APPLIED 6b\n", []string{"old"}},
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT 6b 6e6577\nTARP_ERROR_COMMITTED 6b\n", []string{"new"}},
		{"TARP_ACK 6b 6f6c64\nTARP_INTENT_DELETE 6b\nTARP_ERROR_COMMITTED 6b\n", []string{"<missing>"}},
	}
	for _, tc := range cases {
		var events []OracleEvent
		for _, line := range strings.Split(strings.TrimSpace(tc.history)+"\nTARP_PHASE data.sync:after", "\n") {
			event, recognized, err := ParseOracle(line)
			if err != nil || !recognized {
				t.Fatalf("parse %q: recognized=%t %v", line, recognized, err)
			}
			events = append(events, event)
		}
		oracle, err := ReplayOracle(events, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range []string{"old", "new", "partial", "<missing>"} {
			_, err := oracle.Verify(func([]byte) ([]byte, bool, error) {
				return []byte(state), state != "<missing>", nil
			})
			allowed := false
			for _, candidate := range tc.allowed {
				allowed = allowed || state == candidate
			}
			if (err == nil) != allowed {
				t.Fatalf("history %q state %q: %v", tc.history, state, err)
			}
		}
	}
}

func TestDefinitiveOracleOutcomesDoNotKeepAnUnknownWindow(t *testing.T) {
	for _, kind := range []string{"not_applied", "error_committed"} {
		events := []OracleEvent{
			{Type: "ack", Key: []byte("base"), Value: []byte("v")},
			{Type: "intent", Key: []byte("new"), Value: []byte("v")},
			{Type: kind, Key: []byte("new")},
			{Type: "phase", Phase: "complete"},
		}
		oracle, err := ReplayOracle(events, false)
		if err != nil {
			t.Fatal(err)
		}
		want := uint64(1)
		if kind == "error_committed" {
			want++
		}
		if low, high := oracle.SequenceRange(); low != want || high != want {
			t.Fatalf("%s left a sequence window [%d,%d]", kind, low, high)
		}
		_, err = oracle.Verify(func(key []byte) ([]byte, bool, error) {
			return []byte("v"), string(key) == "base" || kind == "error_committed", nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := oracle.Verify(func(key []byte) ([]byte, bool, error) {
			return []byte("v"), string(key) == "base" || kind == "not_applied", nil
		}); err == nil {
			t.Fatalf("%s accepted the opposite definitive insertion outcome", kind)
		}
		events[2].Key = []byte("different")
		if _, err := ReplayOracle(events, false); err == nil {
			t.Fatal("accepted a definitive outcome for a different pending key")
		}
	}
	if _, err := ReplayOracle([]OracleEvent{
		{Type: "intent", Key: []byte("new")}, {Type: "not_applied", Key: []byte("new")},
		{Type: "phase", Phase: "complete"},
	}, false); err == nil {
		t.Fatal("an unapplied mutation invented an acknowledged history")
	}
}

func TestMutationPowerCutSequenceRange(t *testing.T) {
	cases, err := CampaignCases("mutate")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		key := []byte("k")
		events := []OracleEvent{{Type: "ack", Key: key, Value: []byte("old")}}
		if tc.Mode == "reinsert" {
			events = append(events, OracleEvent{Type: "ack_delete", Key: key})
		}
		if tc.Mode == "insert" {
			key = []byte("different")
		}
		intent := OracleEvent{Type: "intent", Key: key, Value: []byte("new")}
		ack := OracleEvent{Type: "ack", Key: key, Value: intent.Value}
		if tc.Mode == "delete" {
			intent = OracleEvent{Type: "intent_delete", Key: key}
			ack = OracleEvent{Type: "ack_delete", Key: key}
		}
		old := uint64(len(events))
		events = append(events, intent)
		wantLow, wantHigh := old, old
		switch tc.Phase {
		case "current.rename:after", "current.dirsync:before":
			wantHigh = old + 1
		case "operation.ack:after":
			events = append(events, ack)
			fallthrough
		case "current.dirsync:after":
			wantLow, wantHigh = old+1, old+1
		}
		oracle, err := ReplayOracle(append(events, OracleEvent{Type: "phase", Phase: tc.Phase}), false)
		if err != nil {
			t.Fatal(err)
		}
		if lo, hi := oracle.PowerCutSequenceRange(tc.Mode); lo != wantLow || hi != wantHigh {
			t.Errorf("%s: got [%d,%d], want [%d,%d]", tc.Name, lo, hi, wantLow, wantHigh)
		}
		if tc.Phase == "current.dirsync:after" {
			if lo, hi := oracle.PowerCutSequenceRange("overlap"); lo != old || hi != old+1 {
				t.Errorf("unrelated publication constrained pending write to [%d,%d]", lo, hi)
			}
		}
	}
}

func TestOracleRejectsAppliedDeletesOfAbsentKeys(t *testing.T) {
	live, missing := []byte("live"), []byte("missing")
	batch := []OracleMutation{{Key: live, Delete: true}, {Key: missing, Delete: true}}
	for _, events := range [][]OracleEvent{
		{{Type: "ack_delete", Key: missing}},
		{{Type: "intent_delete", Key: missing}, {Type: "error_committed", Key: missing}},
		{{Type: "batch_intent", Mutations: batch}, {Type: "batch_ack", Mutations: batch}},
		{{Type: "batch_intent", Mutations: batch}, {Type: "not_applied"}},
	} {
		history := append([]OracleEvent{{Type: "ack", Key: live}}, events...)
		if _, err := ReplayOracle(append(history, OracleEvent{Type: "phase", Phase: "cut"}), false); err == nil {
			t.Fatalf("accepted invalid outcome for history %+v", history)
		}
	}
	oracle, err := ReplayOracle([]OracleEvent{
		{Type: "ack", Key: live}, {Type: "batch_intent", Mutations: batch}, {Type: "phase", Phase: "cut"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if low, high := oracle.SequenceRange(); low != 1 || high != 1 {
		t.Fatalf("pending BatchDelete of an absent key advanced sequence to [%d,%d]", low, high)
	}
}

func TestColdOracleBindsSequenceContentAndEntireLiveCardinality(t *testing.T) {
	for _, pending := range []OracleEvent{
		{Type: "intent", Key: []byte("k"), Value: []byte("new")},
		{Type: "intent", Key: []byte("n"), Value: []byte("new")},
		{Type: "intent_delete", Key: []byte("k")},
	} {
		oracle, err := ReplayOracle([]OracleEvent{
			{Type: "ack", Key: []byte("k"), Value: []byte("old")},
			pending, {Type: "phase", Phase: "cut"},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, sequence := range []uint64{1, 2} {
			state, err := oracle.StateAt(sequence)
			if err != nil {
				t.Fatal(err)
			}
			lookup := func(key []byte) ([]byte, bool, error) {
				value, present := state[string(key)]
				return value, present, nil
			}
			digest, err := oracle.VerifyAt(sequence, len(state), lookup)
			if err != nil {
				t.Fatal(err)
			}
			expected, count, err := oracle.DigestAt(sequence)
			if err != nil || digest != expected || count != len(state) {
				t.Fatalf("external digest differs: %v", err)
			}
			if _, err := oracle.VerifyAt(3-sequence, len(state), lookup); err == nil {
				t.Fatalf("accepted opposite pending state at sequence %d: %+v", 3-sequence, pending)
			}
			if _, err := oracle.VerifyAt(sequence, len(state)+1, lookup); err == nil {
				t.Fatal("accepted an unexpected live key outside the witnessed set")
			}
		}
	}
}

// This is the JSON representation used by existing qualification evidence.
// A deleted key remains witnessed, and repeated pending Puts yield one key.
func TestOracleDigestPreservesWitnessedKeysAndReusedReadBuffers(t *testing.T) {
	oracle, err := ReplayOracle([]OracleEvent{
		{Type: "ack", Key: []byte{0xff}, Value: []byte("3")},
		{Type: "ack", Key: []byte("z")},
		{Type: "ack_delete", Key: []byte("z")},
		{Type: "batch_intent", Mutations: []OracleMutation{
			{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("a"), Value: []byte("2")},
		}},
		{Type: "phase", Phase: "cut"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	const jsonState = `[{"key":"YQ==","value":"Mg==","present":true},{"key":"eg==","present":false},{"key":"/w==","value":"Mw==","present":true}]`
	sum := sha256.Sum256([]byte(jsonState))
	want := hex.EncodeToString(sum[:])
	digest, live, err := oracle.DigestAt(5)
	if err != nil || live != 2 || digest != want {
		t.Fatalf("DigestAt = %s, %d, %v, want %s with two live keys", digest, live, err, want)
	}

	// A lookup can reuse its buffer on the next call.
	var scratch [1]byte
	lookup := func(key []byte) ([]byte, bool, error) {
		switch string(key) {
		case "a":
			scratch[0] = '2'
		case "\xff":
			scratch[0] = '3'
		default:
			return nil, false, nil
		}
		return scratch[:], true, nil
	}
	if got, err := oracle.VerifyAt(5, live, lookup); err != nil || got != want {
		t.Fatalf("VerifyAt = %s, %v, want %s", got, err, want)
	}
	if got, err := oracle.Verify(lookup); err != nil || got != want {
		t.Fatalf("Verify = %s, %v, want %s", got, err, want)
	}
}

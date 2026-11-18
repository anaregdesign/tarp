package qualification

import (
	"strings"
	"testing"
)

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

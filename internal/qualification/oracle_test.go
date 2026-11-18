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

package qualification

import (
	"strings"
	"testing"
)

func TestProbeConsoleFramingAndVerdict(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		pass        bool
	}{
		{"raw", "TARP_ENVIRONMENT_PASS\r\nTARP_PROBE_EXIT 0\r\n", true},
		{"framed-markers-ignored", "[ 201.387 ] cloud-init[905]: TARP_ENVIRONMENT_PASS\n[ 201.401 ] cloud-init[905]: TARP_PROBE_EXIT 0\n", false},
		{"traced-commands-ignored", "+ echo TARP_ENVIRONMENT_PASS\n+ echo TARP_PROBE_EXIT 0\n", false},
		{"embedded-text-ignored", "command: echo TARP_ENVIRONMENT_PASS\nresult: TARP_PROBE_EXIT 0\n", false},
		{"missing-exit", "TARP_ENVIRONMENT_PASS\n", false},
		{"failed-exit", "TARP_ENVIRONMENT_PASS\nTARP_PROBE_EXIT 1\n", false},
		{"exit-without-pass", "TARP_PROBE_EXIT 0\n", false},
		{"extra-payload", "TARP_ENVIRONMENT_PASS\nTARP_PROBE_EXIT 0 ignored\n", false},
		{"duplicate-pass", "TARP_ENVIRONMENT_PASS\nTARP_ENVIRONMENT_PASS\nTARP_PROBE_EXIT 0\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyProbe(strings.NewReader(tc.input))
			if (err == nil) != tc.pass {
				t.Fatalf("pass=%t error=%v", tc.pass, err)
			}
		})
	}
}

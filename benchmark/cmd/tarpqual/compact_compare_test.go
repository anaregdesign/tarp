package main

import (
	"slices"
	"testing"
)

func TestFixedCompactTrialsAlternateWithoutVariableWindows(t *testing.T) {
	trials, err := compactTrials("candidate-fixed")
	if err != nil {
		t.Fatal(err)
	}
	if len(trials) != 13 || trials[0] != (compactTrial{"base", "seed", 0}) {
		t.Fatalf("unexpected fixed schedule: %+v", trials)
	}
	for i, trial := range trials[1:] {
		round := i/2 + 1
		arm := "base"
		if i%4 == 1 || i%4 == 2 {
			arm = "next"
		}
		if trial != (compactTrial{arm, "fixed", round}) {
			t.Fatalf("trial %d: %+v", i, trial)
		}
	}
}

func TestCompactTrialsPinnedControlsAndDiagnosticWindows(t *testing.T) {
	for _, tc := range []struct {
		pair, diagnostic string
		controls         []compactTrial
	}{
		{"candidate", "lock-profile", []compactTrial{
			{"base", "baseline", 0}, {"next", "baseline", 0}, {"next", "pinned", 0},
		}},
		{"releases", "diagnostic", []compactTrial{
			{"base", "baseline", 0}, {"next", "baseline", 0}, {"next", "pinned", 0}, {"base", "pinned", 0},
		}},
	} {
		t.Run(tc.pair, func(t *testing.T) {
			const controlsStart = 1 + 6*4 // Seed followed by six rounds of two paired windows.
			diagnosticStart := controlsStart + len(tc.controls)
			trials, err := compactTrials(tc.pair)
			if err != nil || len(trials) != diagnosticStart+4*2 {
				t.Fatalf("schedule: %d rows, %v", len(trials), err)
			}
			if got := trials[controlsStart:diagnosticStart]; !slices.Equal(got, tc.controls) {
				t.Fatalf("controls: got %+v, want %+v", got, tc.controls)
			}
			for i, trial := range trials[diagnosticStart:] {
				arm := "base"
				if i%4 == 1 || i%4 == 2 {
					arm = "next"
				}
				if trial != (compactTrial{arm, tc.diagnostic, i/2 + 1}) {
					t.Fatalf("diagnostic window %d: %+v", i, trial)
				}
			}
		})
	}
	if _, err := compactTrials("typo"); err == nil {
		t.Fatal("unknown comparison pair accepted")
	}
}

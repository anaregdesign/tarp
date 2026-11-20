package main

import "testing"

func TestFixedCompactTrialsAlternateWithoutVariableWindows(t *testing.T) {
	trials := compactTrials(true)
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

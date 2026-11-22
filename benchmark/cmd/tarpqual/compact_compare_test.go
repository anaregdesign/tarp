package main

import "testing"

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

func TestCompactTrialsRequireTheDiagnosticForEachPair(t *testing.T) {
	for pair, mode := range map[string]string{"candidate": "lock-profile", "releases": "diagnostic"} {
		trials, err := compactTrials(pair)
		if err != nil || len(trials) != 37 {
			t.Fatalf("%s schedule: %d rows, %v", pair, len(trials), err)
		}
		for i, trial := range trials[29:] {
			arm := "base"
			if i%4 == 1 || i%4 == 2 {
				arm = "next"
			}
			if trial != (compactTrial{arm, mode, i/2 + 1}) {
				t.Fatalf("%s diagnostic window %d: %+v", pair, i, trial)
			}
		}
	}
	if _, err := compactTrials("typo"); err == nil {
		t.Fatal("unknown comparison pair accepted")
	}
}

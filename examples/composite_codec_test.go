package examples_test

import (
	"fmt"
	"time"

	"github.com/anaregdesign/tarp"
)

func Example_compositeCodec() {
	type Run struct {
		Started  time.Time
		Attempts []time.Duration
		Labels   map[string]string
	}
	codec, err := tarp.NewCodec[Run]()
	if err != nil {
		panic(err)
	}
	data, err := codec.Encode(Run{
		Started:  time.Date(2026, 11, 19, 9, 30, 0, 0, time.UTC),
		Attempts: []time.Duration{200 * time.Millisecond, 150 * time.Millisecond},
		Labels:   map[string]string{"job": "backup"},
	})
	if err != nil {
		panic(err)
	}
	run, err := codec.Decode(data)
	if err != nil {
		panic(err)
	}
	fmt.Println(run.Labels["job"], run.Started.Format(time.RFC3339), run.Attempts)
	// Output: backup 2026-11-19T09:30:00Z [200ms 150ms]
}

package qualification

import "encoding/json"

const (
	MeasurementKeys = 4096
	MeasurementReps = 5
)

type MeasurementConfig struct {
	ValueBytes int  `json:"value_bytes"`
	Profile    bool `json:"profile"`
}

type MeasurementRow struct {
	Kind       string          `json:"kind"`
	ValueBytes int             `json:"value_bytes"`
	Repetition int             `json:"repetition"`
	Data       json.RawMessage `json:"data"`
}

type MeasurementVerdict struct {
	ValueBytes       int  `json:"value_bytes"`
	Profile          bool `json:"profile"`
	WorkingKeys      int  `json:"working_keys"`
	Repetitions      int  `json:"repetitions"`
	Rows             int  `json:"rows"`
	ColdCalls        int  `json:"cold_calls"`
	ColdOpens        int  `json:"cold_opens"`
	HistoryMutations int  `json:"history_mutations"`
}

func MeasurementSize(size int) bool { return size == 64 || size == 1024 || size == 4096 }

package qualification

import "errors"

type PagerVerdict struct {
	DeviceBytes          int64  `json:"device_bytes"`
	FillerBytes          int64  `json:"filler_bytes"`
	Errno                int    `json:"errno"`
	Outcome              string `json:"outcome"`
	Phase                string `json:"phase"`
	DataWrites           int    `json:"data_writes"`
	PrepareSteps         int    `json:"prepare_steps"`
	ScratchWriteENOSPC   bool   `json:"scratch_write_enospc"`
	Healthy              bool   `json:"healthy"`
	SequenceBefore       uint64 `json:"sequence_before"`
	SequenceAfterFailure uint64 `json:"sequence_after_failure"`
	SequenceAfterRetry   uint64 `json:"sequence_after_retry"`
	BatchRecords         int    `json:"batch_records"`
	RetryPassed          bool   `json:"retry_passed"`
}

func (v PagerVerdict) Validate() error {
	if v.DeviceBytes != MaxDisk || v.FillerBytes <= MaxDisk/2 || v.FillerBytes > MaxDisk ||
		v.Errno != 28 || v.Outcome != "not-applied" || v.Phase != "commit" || v.DataWrites != 0 ||
		v.PrepareSteps <= 0 || !v.ScratchWriteENOSPC ||
		!v.Healthy || v.SequenceBefore == 0 || v.SequenceAfterFailure != v.SequenceBefore ||
		v.BatchRecords != 1536 || v.SequenceAfterRetry != v.SequenceBefore+uint64(v.BatchRecords) || !v.RetryPassed {
		return errors.New("qualification: incomplete private-pager exhaustion/retry verdict")
	}
	return nil
}

type ExhaustionVerdict struct {
	DeviceBytes            int64  `json:"device_bytes"`
	FirstFillerBytes       int64  `json:"first_filler_bytes"`
	FirstFillerAllocated   int64  `json:"first_filler_allocated_bytes"`
	FirstFailurePoint      string `json:"first_failure_point"`
	FirstErrno             int    `json:"first_errno"`
	FullAvailableBytes     uint64 `json:"full_available_bytes"`
	CheckpointAvailable    uint64 `json:"checkpoint_available_bytes"`
	CheckpointError        string `json:"checkpoint_error"`
	CheckpointNotApplied   bool   `json:"checkpoint_not_applied"`
	CheckpointRetryPassed  bool   `json:"checkpoint_retry_passed"`
	PinnedBytes            int64  `json:"pinned_bytes"`
	ReleasedPinnedBytes    int64  `json:"released_pinned_bytes"`
	ReclaimedAfterRelease  int64  `json:"reclaimed_after_release_bytes"`
	FinalFillerBytes       int64  `json:"final_filler_bytes"`
	MutationError          string `json:"mutation_error"`
	MutationOutcome        string `json:"mutation_outcome"`
	MutationErrno          int    `json:"mutation_errno"`
	HotAndMissFailStop     bool   `json:"hot_and_miss_fail_stop"`
	RepeatedCloseSameError bool   `json:"repeated_close_same_error"`
	ReopenedAndVerified    bool   `json:"reopened_and_verified"`
}

func (v ExhaustionVerdict) Validate() error {
	if v.DeviceBytes != MaxDisk || v.FirstFillerBytes <= MaxDisk/2 || v.FirstFillerBytes > MaxDisk ||
		v.FirstFillerAllocated <= MaxDisk/2 || v.FirstFillerAllocated > MaxDisk ||
		v.FirstErrno != 28 || v.MutationErrno != 28 ||
		v.FirstFailurePoint != "write" && v.FirstFailurePoint != "sync" ||
		v.CheckpointAvailable < 128<<10 || v.CheckpointAvailable >= 256<<10 ||
		v.CheckpointError == "" || !v.CheckpointNotApplied || !v.CheckpointRetryPassed ||
		v.PinnedBytes <= 0 || v.ReleasedPinnedBytes != 0 || v.ReclaimedAfterRelease <= 0 ||
		v.FinalFillerBytes <= MaxDisk/2 || v.FinalFillerBytes > MaxDisk || v.MutationError == "" ||
		v.MutationOutcome != "not-applied" && v.MutationOutcome != "committed" && v.MutationOutcome != "unknown" ||
		!v.HotAndMissFailStop || !v.RepeatedCloseSameError || !v.ReopenedAndVerified {
		return errors.New("qualification: incomplete real bounded-filesystem exhaustion verdict")
	}
	return nil
}

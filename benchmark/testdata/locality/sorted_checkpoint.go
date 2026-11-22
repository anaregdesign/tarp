// This file is copied into a temporary build of Tarp's storage package only
// by the key-locality experiment. The bounded fixture fits in memory.
package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

func (c *Checkpoint) localityReplaySorted(ctx context.Context, history index.Backend, live index.View, emit func(string, Location) (io.Writer, error)) error {
	type row struct {
		key    string
		old    Location
		record *bytes.Buffer
	}
	var rows []row
	var encodedBytes int64
	err := c.capture.replayLive(ctx, c.store.IO, history, live, func(key string, old Location) (io.Writer, error) {
		if old.Bytes > (32<<20)-encodedBytes || len(rows) >= 1<<16 {
			return nil, errors.New("locality experiment: fixture exceeds in-memory sorter limit")
		}
		encodedBytes += old.Bytes
		record := bytes.NewBuffer(make([]byte, 0, int(old.Bytes)))
		rows = append(rows, row{key, old, record})
		return record, nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(rows, func(a, b row) int { return strings.Compare(a.key, b.key) })
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if int64(r.record.Len()) != r.old.Bytes || r.record.Len() <= format.RecordHeaderBytes {
			return errors.New("locality experiment: incomplete replayed Put")
		}
		out, err := emit(r.key, r.old)
		if err != nil {
			return err
		}
		if _, err := r.record.WriteTo(out); err != nil {
			return err
		}
	}
	return nil
}

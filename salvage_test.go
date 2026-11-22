package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestSalvageDataResumesAfterDamagedRecordHeaders(t *testing.T) {
	ctx, dir := t.Context(), t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const block = 4096
	entries := make([]tarp.Entry, 18)
	for i := range entries {
		size := block
		switch i {
		case 0:
			size -= 64 // segment header
		case 1:
			size *= 2 // its continuation in block 2 must never be mistaken for a record
		}
		key := []byte{byte('A' + i)}
		entries[i] = tarp.Entry{Key: key, Value: bytes.Repeat(key, size-48-len(key))}
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	names, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(names) != 1 {
		t.Fatalf("expected one protected file, got %v, %v", names, err)
	}
	base := filepath.Base(names[0])
	id, err := strconv.ParseUint(base[strings.LastIndexByte(base, '-')+1:], 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	dataName := filepath.Join(dir, strings.Replace(base, "PARITY-", "DATA-", 1))
	data, err := os.ReadFile(dataName)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 3, 16} {
		data[index*block+24] ^= 0x80 // original length bytes at these record headers
	}
	if err := os.WriteFile(dataName, data, 0600); err != nil {
		t.Fatal(err)
	}
	var got []tarp.SalvagedRecord
	report, err := tarp.SalvageData(ctx, dir, id, 0, func(v tarp.SalvagedRecord) error { got = append(got, v); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(report.UnrecoverableByteRanges) != 2 || report.UnrecoverableByteRanges[0] != (tarp.DataByteRange{Start: block, End: 2 * block}) || report.UnrecoverableByteRanges[1] != (tarp.DataByteRange{Start: 3 * block, End: 4 * block}) {
		t.Fatalf("unexpected missing ranges %v", report.UnrecoverableByteRanges)
	}
	if len(got) != len(entries)-2 || report.Records != uint64(len(got)) {
		t.Fatalf("recovered %d of %d, report %+v", len(got), len(entries), report)
	}
	for i, rec := range got {
		j := i
		if i > 0 {
			j += 2
		}
		if rec.Deleted || rec.Sequence != uint64(j+1) || !bytes.Equal(rec.Key, entries[j].Key) || !bytes.Equal(rec.Value, entries[j].Value) {
			t.Fatalf("unexpected recovered record %d at %d, seq %d", i, rec.Offset, rec.Sequence)
		}
	}
	if after, err := os.ReadFile(dataName); err != nil || !bytes.Equal(after, data) {
		t.Fatalf("salvage modified source: %v", err)
	}
	stop := errors.New("consumer stopped")
	if _, err := tarp.SalvageData(ctx, dir, id, 0, func(tarp.SalvagedRecord) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error: %v", err)
	}
	if _, err := tarp.SalvageData(ctx, dir, id, 100, func(tarp.SalvagedRecord) error { return nil }); !errors.Is(err, tarp.ErrIncompatibleOptions) {
		t.Fatalf("small record limit: %v", err)
	}
}

func TestSalvageDataRequiresValidArgumentsAndClosedDatabase(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	visit := func(tarp.SalvagedRecord) error { return nil }
	if _, err := tarp.SalvageData(t.Context(), dir, 1, 0, visit); !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("open database: %v", err)
	}
	if _, err := tarp.SalvageData(t.Context(), dir, 0, 0, visit); !errors.Is(err, tarp.ErrInvalidOptions) {
		t.Fatalf("zero file ID: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := tarp.SalvageData(ctx, dir, 1, 0, visit); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestSalvageDataDistinguishesEmptyPutAndDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("a")
	if err := db.Put(key, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	// Keep enough live data that automatic cleaning retains this physical log.
	if err := db.Put([]byte("b"), bytes.Repeat([]byte("v"), 4096)); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("PARITY sidecars %v, %v", files, err)
	}
	name := filepath.Base(files[0])
	id, err := strconv.ParseUint(name[strings.LastIndexByte(name, '-')+1:], 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	var got []tarp.SalvagedRecord
	report, err := tarp.SalvageData(t.Context(), dir, id, 0, func(v tarp.SalvagedRecord) error { got = append(got, v); return nil })
	if err != nil || report.Records != 3 || len(report.UnrecoverableByteRanges) != 0 {
		t.Fatalf("report: %+v, %v", report, err)
	}
	if string(got[0].Key) != "a" || got[0].Deleted || len(got[0].Value) != 0 || string(got[1].Key) != "a" || !got[1].Deleted || len(got[1].Value) != 0 {
		t.Fatalf("empty Put and Delete confused: %v", got[:2])
	}
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err = tarp.SalvageData(ctx, dir, id, 0, func(tarp.SalvagedRecord) error {
		calls++
		cancel()
		return nil
	})
	cancel()
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("canceled visitor received %d records: %v", calls, err)
	}
	fd, err := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fd.Write([]byte{0}); err != nil {
		fd.Close()
		t.Fatal(err)
	}
	if err = fd.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = tarp.SalvageData(t.Context(), dir, id, 0, func(tarp.SalvagedRecord) error { t.Fatal("invalid sidecar emitted a record"); return nil })
	if !errors.Is(err, tarp.ErrInvalidParity) {
		t.Fatalf("invalid sidecar %v", err)
	}
}

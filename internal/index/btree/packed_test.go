package btree

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

func packedAddress(k string, seq uint64) index.Address {
	return index.Address{FileID: 1, Offset: 64 + int64(seq)*64, Bytes: int64(format.RecordHeaderBytes+len(k)) + 3, ValueBytes: 3, Sequence: seq}
}

func TestPackedReopenSupportsWritesAndPinnedReaders(t *testing.T) {
	tr := openTest(t)
	want := make(map[string]index.Address)
	keys := []string{"\x00", "\x00\xff"}
	for i := range 1800 {
		key := fmt.Sprintf("s%08d", i)
		if i < 260 {
			key = strings.Repeat("\x00\xff", 4110) + key
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
		for i, key := range keys {
			want[key] = packedAddress(key, uint64(i+1))
			if err := put(key, want[key]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	if err := ExportPacked(t.Context(), tr, &data); err != nil {
		t.Fatal(err)
	}
	n := 0
	backend, err := ImportPacked(t.Context(), &data, int64(data.Len()), t.TempDir(), MinBudget, func(k string, a index.Address) error {
		if n >= len(keys) || keys[n] != k || want[k] != a {
			t.Fatal("incorrect packed entry")
		}
		n++
		return nil
	})
	if err != nil || n != len(keys) || data.Len() != 0 {
		t.Fatalf("import visited %d keys, %d bytes left: %v", n, data.Len(), err)
	}
	defer backend.Close()
	old := backend.View()
	defer old.Close()
	x := backend.Begin()
	if err := x.Delete(keys[10]); err != nil {
		t.Fatal(err)
	}
	if err := x.Put("new", packedAddress("new", 9999)); err != nil {
		t.Fatal(err)
	}
	x.Commit()
	for _, key := range []string{keys[10], keys[50], keys[len(keys)-1]} {
		a, ok, err := old.Lookup(key)
		if !ok || err != nil || a != want[key] {
			t.Fatalf("pinned root lost %q: %v, %v", key[:min(10, len(key))], ok, err)
		}
	}
	delete(want, keys[10])
	want["new"] = packedAddress("new", 9999)
	now := backend.View()
	defer now.Close()
	verify(t, now, want)
	if err := ExportPacked(t.Context(), backend, io.Discard); err == nil {
		t.Fatal("exported a mutated pager with free or retired pages")
	}
}

func TestPackedEmptyGoldenImage(t *testing.T) {
	const golden = "544152504254503100200000aa36918a0000000000000000000000000000000000000000000000000000000000000000"
	b, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}
	tr := openTest(t)
	var out bytes.Buffer
	if err := ExportPacked(t.Context(), tr, &out); err != nil || !bytes.Equal(b, out.Bytes()) {
		t.Fatalf("empty image = %x, %v", out.Bytes(), err)
	}
	backend, err := ImportPacked(t.Context(), bytes.NewReader(b), int64(len(b)), t.TempDir(), MinBudget, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if backend.Count() != 0 {
		t.Fatal("non-empty imported tree")
	}
	x := backend.Begin()
	if err := x.Put("a", packedAddress("a", 1)); err != nil {
		t.Fatal(err)
	}
	x.Commit()
	if _, ok, err := backend.Lookup("a"); !ok || err != nil {
		t.Fatalf("first post-import write: %v, %v", ok, err)
	}
}

func leafCell(k string, seq uint64) cell {
	return cell{key: keyRef{inline: k, length: uint64(len(k))}, address: packedAddress(k, seq)}
}

func packedBytes(h packedHeader, pp ...*page) []byte {
	b := h.encode()
	data := append([]byte(nil), b[:]...)
	for _, p := range pp {
		data = append(data, p[:]...)
	}
	return data
}

func packedThreePages() (packedHeader, []*page) {
	left := node{kind: leaf, cells: []cell{leafCell("a", 1), leafCell("b", 2)}}
	right := node{kind: leaf, cells: []cell{leafCell("e", 3)}}
	root := node{kind: branch, left: 1, cells: []cell{{key: keyRef{inline: "c", length: 1}, child: 2}}}
	return packedHeader{root: 3, pages: 3, keys: 3, maxKey: 1}, []*page{left.encode(1), right.encode(2), root.encode(3)}
}

func TestPackedRejectsDamagedImagesBeforePublication(t *testing.T) {
	for _, tc := range []string{
		"truncated input", "trailing page", "unsupported codec", "bad header CRC", "bad page CRC",
		"missing root", "oversize keys", "incorrect count", "incorrect maximum", "repeated child",
		"branch cycle", "out-of-range child", "unreachable page", "unequal leaf depths",
		"equal left boundary", "above right boundary", "bad run length", "address overflow",
		"missing file", "missing sequence", "wrong value length", "leaf link", "overflow in tree",
	} {
		t.Run(tc, func(t *testing.T) {
			h, pp := packedThreePages()
			switch tc {
			case "missing root":
				h.root = 0
			case "oversize keys":
				h.maxKey = math.MaxUint64
			case "incorrect count":
				h.keys--
			case "incorrect maximum":
				h.maxKey++
			case "repeated child":
				pp[2].setChild(1, 1)
			case "branch cycle":
				pp[2].setChild(0, 3)
			case "out-of-range child":
				pp[2].setChild(0, 4)
			case "unreachable page":
				h.pages++
				pp = append(pp, (node{kind: leaf, cells: []cell{leafCell("x", 4)}}).encode(4))
			case "unequal leaf depths":
				h.pages++
				pp[2].setChild(0, 4)
				pp = append(pp, (node{kind: branch, left: 1}).encode(4))
			case "equal left boundary", "above right boundary":
				n := pp[2].decode()
				n.cells[0].key.inline = "b"
				if tc == "above right boundary" {
					n.cells[0].key.inline = "f"
				}
				pp[2] = n.encode(3)
			case "bad run length", "address overflow", "missing file", "missing sequence", "wrong value length":
				n := pp[0].decode()
				a := &n.cells[0].address
				switch tc {
				case "bad run length":
					a.Bytes = 3
				case "address overflow":
					a.Offset = math.MaxInt64
				case "missing file":
					a.FileID = 0
				case "missing sequence":
					a.Sequence = 0
				case "wrong value length":
					a.ValueBytes++
				}
				pp[0] = n.encode(1)
			case "leaf link":
				order.PutUint64(pp[0][24:], 2)
			case "overflow in tree":
				p := new(page)
				p.initialize(overflow, 1)
				order.PutUint16(p[6:], 1)
				pp[0] = p
			}
			for _, p := range pp {
				p.seal()
			}
			data := packedBytes(h, pp...)
			size := int64(len(data))
			switch tc {
			case "truncated input":
				data = data[:len(data)-1]
			case "trailing page":
				size += pageSize
			case "unsupported codec":
				data[0] = 0
			case "bad header CRC":
				data[12] ^= 0xff
			case "bad page CRC":
				data[len(data)-1] ^= 1
			}
			got, err := ImportPacked(t.Context(), bytes.NewReader(data), size, t.TempDir(), MinBudget, nil)
			if got != nil || !errors.Is(err, ErrInvalidPacked) {
				t.Fatalf("damaged image returned %v", err)
			}
		})
	}
}

func TestPackedRequiresExclusiveCompleteOverflowChains(t *testing.T) {
	k := strings.Repeat("x", pageSize+maxInline)
	ref := keyRef{inline: k[:longPrefix], length: uint64(len(k)), tail: 2}
	for _, tc := range []string{"missing tail", "wrong tail length", "cycle", "shared tail"} {
		t.Run(tc, func(t *testing.T) {
			n := node{kind: leaf, cells: []cell{{key: ref, address: packedAddress(k, 1)}}}
			p2, p3 := new(page), new(page)
			p2.initialize(overflow, 2)
			p3.initialize(overflow, 3)
			order.PutUint16(p2[6:], pageSize-header)
			order.PutUint64(p2[24:], 3)
			order.PutUint16(p3[6:], uint16(len(k)-longPrefix-(pageSize-header)))
			switch tc {
			case "missing tail":
				order.PutUint64(p2[24:], 0)
			case "wrong tail length":
				order.PutUint16(p3[6:], 1)
			case "cycle":
				order.PutUint64(p3[24:], 2)
			case "shared tail":
				ref2 := ref
				ref2.inline = "z" + ref.inline[1:]
				n.cells = append(n.cells, cell{key: ref2, address: packedAddress(k, 2)})
			}
			p2.seal()
			p3.seal()
			data := packedBytes(packedHeader{1, 3, uint64(len(n.cells)), ref.length}, n.encode(1), p2, p3)
			got, err := ImportPacked(t.Context(), bytes.NewReader(data), int64(len(data)), t.TempDir(), MinBudget, nil)
			if got != nil || !errors.Is(err, ErrInvalidPacked) {
				t.Fatalf("invalid overflow chain: %v", err)
			}
		})
	}
}

func TestPackedValidationSpillsPageOwnershipAndPreservesWindow(t *testing.T) {
	tr := openTest(t)
	tr.p.top = 150000
	s := packedSeen{}
	if err := s.open(tr.p); err != nil {
		t.Fatal(err)
	}
	defer s.file.Close()
	for _, id := range []uint64{32768, 32769, 70001, 3, 100000, 1} {
		if err := s.claim(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{100000, 32769, 1, 70001, 32768, 3} {
		if err := s.claim(id); !errors.Is(err, ErrInvalidPacked) {
			t.Fatalf("accepted duplicate page %d: %v", id, err)
		}
	}
}

func TestPackedErrorsPreserveReaderVisitorAndCancellationCauses(t *testing.T) {
	h, pp := packedThreePages()
	data := packedBytes(h, pp...)
	wantErr := errors.New("read failed")
	r := io.MultiReader(bytes.NewReader(data[:packedHeaderBytes]), iotest.ErrReader(wantErr))
	dir := t.TempDir()
	if got, err := ImportPacked(t.Context(), r, int64(len(data)), dir, MinBudget, nil); got != nil || !errors.Is(err, wantErr) || errors.Is(err, ErrInvalidPacked) {
		t.Fatalf("reader error: %v", err)
	}
	for _, tc := range []string{"visitor", "cancellation"} {
		ctx, cancel := context.WithCancel(t.Context())
		expected := wantErr
		if tc == "cancellation" {
			expected = context.Canceled
		}
		got, err := ImportPacked(ctx, bytes.NewReader(data), int64(len(data)), dir, MinBudget, func(string, index.Address) error {
			if tc == "cancellation" {
				cancel()
				return nil
			}
			return wantErr
		})
		cancel()
		if got != nil || !errors.Is(err, expected) || errors.Is(err, ErrInvalidPacked) {
			t.Fatalf("%s error: %v", tc, err)
		}
	}
	// A fresh import can reuse the workspace after a partial or canceled one.
	got, err := ImportPacked(t.Context(), bytes.NewReader(data), int64(len(data)), dir, MinBudget, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
}

func FuzzPackedImage(f *testing.F) {
	h, pp := packedThreePages()
	f.Add(packedBytes(h, pp...))
	f.Add(packedBytes(packedHeader{}))
	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > packedHeaderBytes+8*pageSize {
			return
		}
		got, err := ImportPacked(t.Context(), bytes.NewReader(data), int64(len(data)), dir, MinBudget, nil)
		if err == nil {
			if err := got.Close(); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrInvalidPacked) {
			t.Fatal(err)
		}
	})
}

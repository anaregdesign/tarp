package btree

import (
	"errors"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func TestPackedCellBytesAtMergeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		extra       int
		groupPrefix bool
	}{
		{"exact fit", 0, false},
		{"one byte over", 1, false},
		{"sibling prefixes lost on merge", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := node{kind: leaf}
			for i := range 180 {
				key := string([]byte{byte(i), 'z'})
				if tc.groupPrefix {
					key = string([]byte{byte(i / 90)}) + key
				}
				if i == 0 {
					key += strings.Repeat("x", 60+tc.extra)
				}
				n.cells = append(n.cells, cell{key: keyRef{inline: key, length: uint64(len(key))}})
			}
			a := node{kind: leaf, cells: n.cells[:90]}
			b := node{kind: leaf, cells: n.cells[90:]}
			bound := header + a.encode(1).packedCellBytes() + b.encode(2).packedCellBytes()
			if bound != pageSize+tc.extra || bound > n.size() || (!tc.groupPrefix && bound != n.size()) {
				t.Fatalf("merged page lower bound %d, encoded size %d", bound, n.size())
			}
		})
	}
}

func TestPageRejectsOutOfOrderAndDamagedSlots(t *testing.T) {
	n := node{kind: leaf, cells: []cell{{key: keyRef{inline: "ab", length: 2}, address: addr(1)}, {key: keyRef{inline: "ac", length: 2}, address: addr(2)}}}
	for _, tc := range []string{"unordered", "overlapping slot", "bad checksum", "invalid kind", "bad address"} {
		t.Run(tc, func(t *testing.T) {
			p := n.encode(1)
			switch tc {
			case "unordered":
				p[p.offset(1)+1] = 'a'
			case "overlapping slot":
				s := header + int(p[5])
				copy(p[s+2:s+4], p[s:s+2])
			case "invalid kind":
				p[4] = 0xff
			case "bad address":
				p[p.offset(0)+1+1+8+7] = 0xff
			}
			p.seal()
			if tc == "bad checksum" {
				p[10] ^= 1
			}
			err := p.check(1)
			if err == nil {
				err = p.validateOrder(func(uint64) (*page, error) { return nil, errPage })
			}
			if !errors.Is(err, errPage) {
				t.Fatalf("accepted %s", tc)
			}
		})
	}
}
func TestOverflowCycleFailsInBoundedReads(t *testing.T) {
	const length = uint64(1) << 56
	r := stream(keyRef{inline: "x", length: length, tail: 7}, nil)
	r.data = nil
	calls := 0
	r.read = func(id uint64) (*page, error) {
		calls++
		if calls > 8 {
			t.Fatal("cycle not detected")
		}
		p := new(page)
		p.initialize(overflow, id)
		order.PutUint16(p[6:], 1)
		order.PutUint64(p[24:], 7+(id-7+1)%3)
		return p, nil
	}
	for {
		_, err := r.bytes()
		if errors.Is(err, errPage) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		r.data = nil
	}
}

func FuzzPageDecoder(f *testing.F) {
	p := (node{kind: leaf, cells: []cell{{key: keyRef{inline: "a", length: 1}, address: index.Address{FileID: 1}}}}).encode(1)
	f.Add(p[:])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > pageSize {
			return
		}
		var p page
		copy(p[:], data)
		order.PutUint64(p[16:24], 1)
		p.seal()
		if err := p.check(1); err != nil {
			return
		}
		if p[4] == overflow {
			_, _, _ = p.chunk(uint64(p.count()))
			return
		}
		_ = p.validateOrder(func(uint64) (*page, error) { return nil, errPage })
		for i := 0; i < p.count(); i++ {
			if _, err := p.cell(i); err != nil {
				t.Fatal(err)
			}
		}
	})
}

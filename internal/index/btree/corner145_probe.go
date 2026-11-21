//go:build corner145

package btree

// Corner145PageIDs samples the current root , allocated ID high water mark and queued retirement records.
// The caller serializes all foreground and maintenance writes.
func (t *tree) Corner145PageIDs() (uint64, uint64, uint64) {
	t.writer.Lock()
	defer t.writer.Unlock()
	return t.root, t.p.top, uint64((t.retired.end - t.retired.start) / 16)
}

// Corner145IDSets is used only in separate identity-witness episodes. Enumerate
// page IDs in the retirement queue and under the live root (short keys only).
func (t *tree) Corner145IDSets() (map[uint64]bool, map[uint64]bool, error) {
	t.writer.Lock()
	defer t.writer.Unlock()
	queued := make(map[uint64]bool)
	if n := t.retired.end - t.retired.start; n > 0 {
		data := make([]byte, n)
		if _, err := t.retired.file.ReadAt(data, t.retired.start); err != nil {
			return nil, nil, err
		}
		for i := 0; i < len(data); i += 16 {
			queued[order.Uint64(data[i+8:])] = true
		}
	}
	live := make(map[uint64]bool)
	stack := []uint64{t.root}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		live[id] = true
		p, err := t.p.read(id)
		if err != nil {
			return nil, nil, err
		}
		if p[4] == branch {
			for i := 0; i <= p.count(); i++ {
				stack = append(stack, p.child(i))
			}
		}
	}
	return queued, live, nil
}

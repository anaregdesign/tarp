//go:build corner145

package btree

// Corner145PageIDs samples the current root , allocated ID high water mark and queued retirement records.
// The caller serializes all foreground and maintenance writes.
func (t *tree) Corner145PageIDs() (uint64, uint64, uint64) {
	t.writer.Lock()
	defer t.writer.Unlock()
	return t.root, t.p.top, uint64((t.retired.end - t.retired.start) / 16)
}

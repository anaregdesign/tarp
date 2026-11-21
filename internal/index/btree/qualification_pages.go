//go:build tarpcorner145

package btree

// VisitQualificationKeys inspects the published root with writes paused. It
// enumerates branch and leaf keys separately and reports their actual overflow
// page IDs so the test can identify ownership changes across generations.
func (t *tree) VisitQualificationKeys(visit func(isBranch bool, pageID uint64, key string, overflowIDs []uint64)) error {
	t.writer.Lock()
	defer t.writer.Unlock()
	seen := make(map[uint64]bool)
	var walk func(uint64) error
	walk = func(id uint64) error {
		if id == 0 || seen[id] {
			return errPage
		}
		seen[id] = true
		p, err := t.p.read(id)
		if err != nil {
			return err
		}
		isBranch := p[4] == branch
		if !isBranch && p[4] != leaf {
			return errPage
		}
		if isBranch {
			if err := walk(p.link()); err != nil {
				return err
			}
		}
		for i := 0; i < p.count(); i++ {
			c, err := p.cell(i)
			if err != nil {
				return err
			}
			key, err := c.key.string(t.p)
			if err != nil {
				return err
			}
			var chain []uint64
			for tail := c.key.tail; tail != 0; {
				if seen[tail] {
					return errPage
				}
				seen[tail] = true
				chain = append(chain, tail)
				next, err := t.p.read(tail)
				if err != nil {
					return err
				}
				if next[4] != overflow {
					return errPage
				}
				tail = next.link()
			}
			visit(isBranch, id, key, chain)
			if isBranch {
				if err := walk(c.child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(t.root)
}

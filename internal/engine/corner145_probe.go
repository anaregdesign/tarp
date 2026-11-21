//go:build corner145

package engine

// Corner145PageIDs exposes the two diagnostic fields only to this qualification build.
func (o Observer) Corner145PageIDs() (uint64, uint64) {
	return o.db.keys.(interface{ Corner145PageIDs() (uint64, uint64) }).Corner145PageIDs()
}

package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
)

func readCatalog(ctx context.Context, dir string, current format.Frontier) (format.Catalog, uint64, []uint64, error) {
	var edits []format.CatalogEdit
	var chain []uint64
	var bytes uint64
	id, digest := current.ManifestID, current.ManifestDigest
	for {
		if err := ctx.Err(); err != nil {
			return format.Catalog{}, 0, nil, err
		}
		name := ManifestName(current.UUID, id)
		data, err := ReadRegular(filepath.Join(dir, name))
		if err != nil {
			return format.Catalog{}, 0, nil, requiredFileError(name, err)
		}
		if sha256.Sum256(data) != digest {
			return format.Catalog{}, 0, nil, fmt.Errorf("%s: %w: selected catalog digest", name, ErrCorrupt)
		}
		bytes += uint64(len(data))
		chain = append(chain, id)
		if !format.IsCatalogEdit(data) {
			base, err := format.DecodeCatalog(data)
			if err != nil {
				return format.Catalog{}, 0, nil, fmt.Errorf("%s: %w", name, err)
			}
			if len(edits) == 0 {
				return base, bytes, chain, nil
			}
			catalog, err := applyCatalogEdits(base, edits)
			return catalog, bytes, chain, err
		}
		edit, err := format.DecodeCatalogEdit(data)
		if err != nil {
			return format.Catalog{}, 0, nil, fmt.Errorf("%s: %w", name, err)
		}
		if len(edits) == maxCatalogEdits || edit.PreviousID >= id || edit.Catalog.UUID != current.UUID {
			return format.Catalog{}, 0, nil, fmt.Errorf("%s: %w: catalog chain bounds or identity", name, ErrCorrupt)
		}
		edits = append(edits, edit)
		id, digest = edit.PreviousID, edit.PreviousDigest
	}
}

func applyCatalogEdits(catalog format.Catalog, edits []format.CatalogEdit) (format.Catalog, error) {
	entries := CatalogEntries{catalogFromSlice(catalog.Entries)}
	for i := len(edits) - 1; i >= 0; i-- {
		edit := edits[i]
		next := edit.Catalog
		if edit.Start > entries.Len() || edit.Remove > entries.Len()-edit.Start ||
			next.UUID != catalog.UUID || next.CheckpointCutoffSequence != catalog.CheckpointCutoffSequence ||
			next.PrunedThroughSequence < catalog.PrunedThroughSequence {
			return format.Catalog{}, fmt.Errorf("%w: invalid catalog edit", ErrCorrupt)
		}
		if edit.StagedAppend {
			if !validStagedCatalogAppend(catalog, entries, edit) {
				return format.Catalog{}, fmt.Errorf("%w: invalid staged catalog append", ErrCorrupt)
			}
		} else if next.ActiveID != catalog.ActiveID {
			if edit.Start != entries.Len() || edit.Remove != 0 || len(next.Entries) != 1 ||
				next.Entries[0].ID != catalog.ActiveID || next.Entries[0].Kind != format.RawLogKind {
				return format.Catalog{}, fmt.Errorf("%w: invalid catalog rotation", ErrCorrupt)
			}
		}
		entries = entries.splice(edit.Start, edit.Remove, next.Entries)
		catalog = next
	}
	catalog.Entries = entries.Slice(0, entries.Len())
	return catalog, format.ValidateCatalog(catalog)
}

func validStagedCatalogAppend(prior format.Catalog, entries CatalogEntries, edit format.CatalogEdit) bool {
	next := edit.Catalog
	if edit.Start != entries.Len() || edit.Remove != 0 || next.ActiveID <= prior.ActiveID ||
		next.Index != prior.Index || next.PrunedThroughSequence != prior.PrunedThroughSequence {
		return false
	}
	previous := prior.ActiveID - 1
	for _, entry := range next.Entries {
		if entry.Kind != format.RawLogKind || entry.ID <= previous || entry.ID >= next.ActiveID {
			return false
		}
		previous = entry.ID
	}
	// The combined catalog validation checks record lengths, digests and
	// consecutive logical sequence intervals.
	return true
}

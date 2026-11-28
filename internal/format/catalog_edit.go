package format

import "math"

const catalogEditSignature = "TARPMNED"
const stagedCatalogAppendSignature = "TARPMNSA"
const catalogEditHeaderBytes = 56

// MaxCatalogEditBytes covers one bounded publication and makes the staging
// estimate independent of the number of selected DATA files.
const MaxCatalogEditBytes = 32 + catalogEditHeaderBytes + catalogHeaderBytes + catalogEntryBytes

// MaxStagedCatalogEntries bounds the sealed DATA appended in one staged edit.
const MaxStagedCatalogEntries = 1024

const MaxStagedCatalogEditBytes = MaxCatalogEditBytes + (MaxStagedCatalogEntries-1)*catalogEntryBytes

// CatalogEdit changes a consecutive range of the previous catalog. Catalog
// carries the replacement header and the entries inserted at Start.
type CatalogEdit struct {
	PreviousID     uint64
	PreviousDigest [32]byte
	Start, Remove  int
	Catalog        Catalog
	StagedAppend   bool
}

func IsCatalogEdit(b []byte) bool {
	return len(b) >= 8 && (string(b[:8]) == catalogEditSignature || string(b[:8]) == stagedCatalogAppendSignature)
}

func EncodeCatalogEdit(e CatalogEdit) []byte {
	b := make([]byte, catalogEditHeaderBytes, MaxCatalogEditBytes-32)
	order.PutUint64(b[:8], e.PreviousID)
	copy(b[8:40], e.PreviousDigest[:])
	order.PutUint64(b[40:48], uint64(e.Start))
	order.PutUint64(b[48:56], uint64(e.Remove))
	signature := catalogEditSignature
	if e.StagedAppend {
		signature = stagedCatalogAppendSignature
	}
	return envelope(signature, append(b, encodeCatalogPayload(e.Catalog)...))
}

func DecodeCatalogEdit(b []byte) (CatalogEdit, error) {
	var e CatalogEdit
	signature, limit := catalogEditSignature, MaxCatalogEditBytes
	if len(b) >= 8 && string(b[:8]) == stagedCatalogAppendSignature {
		e.StagedAppend = true
		signature, limit = stagedCatalogAppendSignature, MaxStagedCatalogEditBytes
	}
	p, err := payload(b, signature)
	if err != nil {
		return e, err
	}
	if len(b) > limit || len(p) < catalogEditHeaderBytes {
		return e, bad("catalog edit length")
	}
	e.PreviousID = order.Uint64(p[:8])
	e.PreviousDigest = [32]byte(p[8:40])
	start, remove := order.Uint64(p[40:48]), order.Uint64(p[48:56])
	if e.PreviousID == 0 || e.PreviousDigest == [32]byte{} || start > math.MaxInt || remove > math.MaxInt ||
		e.StagedAppend && remove != 0 {
		return e, bad("catalog edit predecessor or bounds")
	}
	e.Start, e.Remove = int(start), int(remove)
	e.Catalog, err = decodeCatalogPayload(p[catalogEditHeaderBytes:])
	return e, err
}

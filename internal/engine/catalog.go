package engine

import (
	"context"
	"errors"

	"github.com/anaregdesign/tarp/internal/publication"
	"github.com/anaregdesign/tarp/internal/storage"
)

func (db *DB) runCatalogRebase(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-db.parity.catalogWake:
		}
		db.writer.Lock()
		if ctx.Err() != nil || db.health() != nil {
			db.writer.Unlock()
			return
		}
		previous := db.store.Current.ManifestID
		err := db.store.RebaseCatalog(func(prepare func() error) error {
			db.writer.Unlock()
			err := prepare()
			db.writer.Lock()
			return errors.Join(err, ctx.Err(), db.health())
		})
		var failure *storage.Fault
		if errors.As(err, &failure) && failure.Outcome == publication.Unknown {
			db.poison(err)
		}
		if db.store.Current.ManifestID != previous {
			db.notifyCleaner()
		}
		db.writer.Unlock()
	}
}

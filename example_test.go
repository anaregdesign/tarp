package tarp_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp"
)

func Example() {
	if err := roundTrip(); err != nil {
		panic(err)
	}
	// Output: value
}

func roundTrip() (result error) {
	parent, err := os.MkdirTemp("", "tarp-example-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(parent)) }()
	db, err := tarp.Open(filepath.Join(parent, "db"), tarp.Options{})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		return err
	}
	backup := filepath.Join(parent, "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		return err
	}
	if err := db.Put([]byte("key"), []byte("later")); err != nil {
		return err
	}
	if _, err := db.Compact(context.Background()); err != nil {
		return err
	}
	restored := filepath.Join(parent, "restored")
	if err := tarp.Restore(context.Background(), backup, restored, tarp.Options{}); err != nil {
		return err
	}
	image, err := tarp.Open(restored, tarp.Options{})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, image.Close()) }()
	value, err := image.Get([]byte("key"))
	if err != nil {
		return err
	}
	fmt.Println(string(value))
	return nil
}

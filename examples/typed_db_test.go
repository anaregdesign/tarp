package examples_test

import (
	"errors"
	"fmt"
	"os"

	"github.com/anaregdesign/tarp"
)

func Example_typedDB() {
	if err := storeScore(); err != nil {
		panic(err)
	}
	// Output: 42
}

func storeScore() (err error) {
	dir, err := os.MkdirTemp("", "tarp-scores-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, tarp.Int64Codec{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	if err := db.Put("alice", 42); err != nil {
		return err
	}
	score, err := db.Get("alice")
	if err != nil {
		return err
	}
	fmt.Println(score)
	return nil
}

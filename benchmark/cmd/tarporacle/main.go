// Command tarporacle records an external disposable-VM test oracle or verifies
// recovered files against it. It never powers off a host or repairs a database.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/benchmark/qualification"
)

type entry = qualification.OracleEvent

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("tarporacle", flag.ContinueOnError)
	record := flags.String("record", "", "create a new controller-side JSONL oracle from guest standard input")
	verify := flags.String("verify", "", "read an immutable controller-side oracle after reboot")
	dir := flags.String("dir", "", "recovered database directory, used only with -verify")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*record == "") == (*verify == "") || *record != "" && *dir != "" || *verify != "" && *dir == "" {
		return errors.New("use -record FILE or -verify FILE -dir DATABASE; see docs/qualification.md")
	}
	if *record != "" {
		fd, err := os.OpenFile(*record, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		parent, err := os.Open(filepath.Dir(*record))
		if err != nil {
			return errors.Join(err, fd.Close())
		}
		err = errors.Join(fd.Sync(), parent.Sync(), parent.Close())
		if err == nil {
			err = recordOracle(input, output, fd)
		}
		return errors.Join(err, fd.Close())
	}
	fd, err := os.Open(*verify)
	if err != nil {
		return err
	}
	events, err := readOracle(fd)
	if err = errors.Join(err, fd.Close()); err != nil {
		return err
	}
	if err := verifyDatabase(*dir, events); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, "PASS: file-only recovery preserves observed acknowledgments and the permitted whole pending mutation")
	return err
}

func protocol(line string) (entry, bool, error) {
	return qualification.ParseOracle(line)
}

func recordOracle(input io.Reader, output io.Writer, fd *os.File) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	encoder := json.NewEncoder(fd)
	for scanner.Scan() {
		event, recognized, err := protocol(scanner.Text())
		if err != nil {
			return err
		}
		if !recognized {
			continue
		}
		if err := encoder.Encode(event); err != nil {
			return err
		}
		if err := fd.Sync(); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(output, "durable "+scanner.Text()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func readOracle(input io.Reader) ([]entry, error) {
	return qualification.ReadOracle(input)
}

func verifyDatabase(dir string, events []entry) (result error) {
	oracle, err := qualification.ReplayOracle(events, false)
	if err != nil {
		return err
	}
	st, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New("recovered database must be an existing directory")
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		return fmt.Errorf("file-only recovery failed: %w", err)
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	_, err = oracle.Verify(func(key []byte) ([]byte, bool, error) {
		value, err := db.Get(key)
		if errors.Is(err, tarp.ErrNotFound) {
			return nil, false, nil
		}
		return value, err == nil, err
	})
	return err
}

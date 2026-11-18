// Command tarporacle records an external disposable-VM test oracle or verifies
// recovered files against it. It never powers off a host or repairs a database.
package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/anaregdesign/tarp"
)

type entry struct {
	Type  string `json:"type"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
	Phase string `json:"phase,omitempty"`
}

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
	_, err = fmt.Fprintln(output, "PASS: file-only recovery preserves the externally observed acknowledgments and permitted pending Put")
	return err
}

func protocol(line string) (entry, bool, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "TARP_") {
		return entry{}, false, nil
	}
	if len(parts) == 2 && parts[0] == "TARP_PHASE" {
		return entry{Type: "phase", Phase: parts[1]}, true, nil
	}
	if len(parts) != 3 || parts[0] != "TARP_ACK" && parts[0] != "TARP_INTENT" {
		return entry{}, false, fmt.Errorf("invalid guest oracle event: %q", line)
	}
	key, err := hex.DecodeString(parts[1])
	if err != nil || len(key) == 0 {
		return entry{}, false, errors.New("oracle keys must be nonempty hexadecimal")
	}
	value := []byte{}
	if parts[2] != "-" {
		value, err = hex.DecodeString(parts[2])
		if err != nil {
			return entry{}, false, err
		}
	}
	kind := "ack"
	if parts[0] == "TARP_INTENT" {
		kind = "intent"
	}
	return entry{Type: kind, Key: key, Value: value}, true, nil
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
	reader := bufio.NewReader(input)
	var events []entry
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF && len(line) == 0 {
			return events, nil
		}
		if err != nil {
			return nil, fmt.Errorf("incomplete oracle line: %w", err)
		}
		var event entry
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
}

func verifyDatabase(dir string, events []entry) (result error) {
	ack := make(map[string]string)
	var pending *entry
	phase := false
	for i := range events {
		event := &events[i]
		switch event.Type {
		case "ack":
			if len(event.Key) == 0 {
				return errors.New("empty acknowledged key")
			}
			ack[string(event.Key)] = string(event.Value)
			if pending != nil {
				if string(event.Key) != string(pending.Key) || string(event.Value) != string(pending.Value) {
					return errors.New("ack does not match pending intent")
				}
				pending = nil
			}
		case "intent":
			if pending != nil || len(event.Key) == 0 {
				return errors.New("one nonempty-key pending Put at a time is required")
			}
			pending = event
		case "phase":
			phase = event.Phase != ""
		default:
			return fmt.Errorf("unknown oracle event: %q", event.Type)
		}
	}
	if !phase || len(ack) == 0 {
		return errors.New("a durably observed acknowledgment and phase barrier are required")
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
	for key, expected := range ack {
		if pending != nil && key == string(pending.Key) {
			continue
		}
		value, err := db.Get([]byte(key))
		if err != nil || string(value) != expected {
			return fmt.Errorf("acknowledged key %x mismatch: %x, %v", key, value, err)
		}
	}
	if pending != nil {
		key := string(pending.Key)
		value, err := db.Get(pending.Key)
		old, existed := ack[key]
		allowed := err == nil && (string(value) == string(pending.Value) || existed && string(value) == old) ||
			errors.Is(err, tarp.ErrNotFound) && !existed
		if !allowed {
			return fmt.Errorf("pending key %x is neither allowed old state nor complete new state: %x, %v", key, value, err)
		}
	}
	return nil
}

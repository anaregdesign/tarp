// Command tarpqual verifies disposable-guest evidence and prepares a bounded
// offline boot image. It never powers off the host.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/anaregdesign/tarp/internal/qualification"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("tarpqual", flag.ContinueOnError)
	probe := flags.String("verify-probe", "", "verify an immutable raw/cloud-init console log")
	initrd := flags.String("append-initrd", "", "pinned original Ubuntu initramfs")
	init := flags.String("init", "", "static disposable-guest PID 1 binary")
	target := flags.String("output", "", "new output initramfs filename")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("qualification: unexpected arguments")
	}
	if *probe != "" && *initrd == "" && *init == "" && *target == "" {
		fd, err := os.Open(*probe)
		if err != nil {
			return err
		}
		if err := errors.Join(qualification.VerifyProbe(fd), fd.Close()); err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(map[string]string{
			"environment_probe": "pass", "power_loss_qualification": "not_run",
			"volatile_cache_controls": "not_run", "device_exhaustion": "not_run",
		})
	}
	if *probe == "" && *initrd != "" && *init != "" && *target != "" {
		return qualification.AppendInitramfs(*initrd, *init, *target)
	}
	return errors.New("use -verify-probe FILE or -append-initrd FILE -init BINARY -output NEW_FILE")
}

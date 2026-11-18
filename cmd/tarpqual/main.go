// Command tarpqual verifies evidence, prepares an offline boot image and runs
// explicitly guarded bounded guests. It never powers off the host.
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
	controls := flags.String("block-controls", "", "prepared disposable hosted-runner work directory")
	campaignDir := flags.String("campaign", "", "prepared disposable hosted-runner work directory")
	runtimeDir := flags.String("runtime-suite", "", "prepared disposable hosted-runner full-suite work directory")
	measurementDir := flags.String("measurements", "", "prepared disposable hosted-runner measurement work directory")
	valueBytes := flags.Int("value-bytes", 0, "measurement value size: 64, 1024 or 4096")
	group := flags.String("group", "", "bounded campaign group (default smoke)")
	selected := flags.String("case", "", "one exact case name within the selected group")
	list := flags.String("list-cases", "", "print the bounded phase matrix without running a VM")
	confirm := flags.String("confirm", "", "explicit disposable-vm-only confirmation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("qualification: unexpected arguments")
	}
	idle := *probe == "" && *initrd == "" && *init == "" && *target == ""
	if *measurementDir != "" {
		if !idle || *controls != "" || *campaignDir != "" || *runtimeDir != "" || *group != "" || *selected != "" || *list != "" {
			return errors.New("measurement-only arguments used with another mode")
		}
		return measurements(*measurementDir, *confirm, *valueBytes, output)
	}
	if *valueBytes != 0 {
		return errors.New("-value-bytes requires measurement mode")
	}
	if *runtimeDir != "" {
		if !idle || *controls != "" || *campaignDir != "" || *group != "" || *selected != "" || *list != "" {
			return errors.New("runtime-only arguments used with another mode")
		}
		return runtimeSuite(*runtimeDir, *confirm, output)
	}
	if *list != "" && idle && *controls == "" && *campaignDir == "" && *group == "" && *selected == "" && *confirm == "" {
		cases, err := qualification.CampaignCases(*list)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(cases)
	}
	if *campaignDir != "" && idle && *controls == "" && *list == "" {
		if *group == "" {
			*group = "smoke"
		}
		return campaign(*campaignDir, *confirm, *group, *selected, output)
	}
	if *campaignDir != "" || *group != "" || *selected != "" || *list != "" {
		return errors.New("campaign-only arguments used with another mode")
	}
	if *controls != "" && idle {
		return blockControls(*controls, *confirm, output)
	}
	if *controls == "" && *confirm == "" && *probe != "" && *initrd == "" && *init == "" && *target == "" {
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
	if *controls == "" && *confirm == "" && *probe == "" && *initrd != "" && *init != "" && *target != "" {
		return qualification.AppendInitramfs(*initrd, *init, *target)
	}
	return errors.New("use -verify-probe FILE, -append-initrd FILE -init BINARY -output NEW_FILE, -list-cases GROUP, or guarded -block-controls/-campaign WORK")
}

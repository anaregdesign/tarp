package qualification

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

var cloudPrefix = regexp.MustCompile(`^\[[ \t]*[0-9]+(?:\.[0-9]+)?[ \t]*\][ \t]+cloud-init\[[0-9]+\]:[ \t]*(.*)$`)

// ConsolePayload recognizes a raw protocol line or the complete cloud-init
// console frame. Shell tracing, quoted commands and embedded substrings do not
// become protocol events.
func ConsolePayload(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if match := cloudPrefix.FindStringSubmatch(line); match != nil {
		line = strings.TrimSpace(match[1])
	}
	return line, strings.HasPrefix(line, "TARP_")
}

func VerifyProbe(input io.Reader) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	passed, exited := false, false
	for scanner.Scan() {
		line, ok := ConsolePayload(scanner.Text())
		if !ok {
			continue
		}
		switch line {
		case "TARP_ENVIRONMENT_PASS":
			if passed || exited {
				return errors.New("qualification: duplicate or out-of-order probe pass")
			}
			passed = true
		case "TARP_PROBE_EXIT 0":
			if !passed || exited {
				return errors.New("qualification: probe exit precedes pass or is duplicated")
			}
			exited = true
		default:
			return fmt.Errorf("qualification: unexpected/failed probe event: %q", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !passed || !exited {
		return errors.New("qualification: explicit environment pass and subsequent successful exit are required")
	}
	return nil
}

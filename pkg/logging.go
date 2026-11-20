package pkg

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

const (
	logReset = "\x1b[0m"
	logCyan  = "\x1b[36m"
	logGreen = "\x1b[32m"
	logBold  = "\x1b[1m"
)

var terminalProgressMu sync.Mutex
var terminalProgressLine string

// SetTerminalProgressLine draws an updating status line on stderr. LogKV
// temporarily clears and redraws it around log messages so the two outputs
// remain readable when they share a terminal.
func SetTerminalProgressLine(line string) {
	terminalProgressMu.Lock()
	defer terminalProgressMu.Unlock()
	terminalProgressLine = line
	fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
}

// ClearTerminalProgressLine ends the updating status line and leaves the
// terminal cursor at the start of the next line.
func ClearTerminalProgressLine() {
	terminalProgressMu.Lock()
	defer terminalProgressMu.Unlock()
	if terminalProgressLine == "" {
		return
	}
	fmt.Fprintln(os.Stderr, "\r\033[K")
	terminalProgressLine = ""
}

// LogKV writes an event as readable key=value fields. Values may be omitted
// from call sites when they contain sensitive query text. Color is automatic
// for terminal output; LOOKUPX_LOG_COLOR=always|never overrides that choice.
func LogKV(event string, fields ...any) {
	terminalProgressMu.Lock()
	defer terminalProgressMu.Unlock()
	progressLine := terminalProgressLine
	if progressLine != "" {
		fmt.Fprint(os.Stderr, "\r\033[K")
	}
	color := logColorEnabled()
	if color {
		event = logBold + logCyan + event + logReset
	}
	parts := make([]string, 0, (len(fields)+1)/2)
	for i := 0; i < len(fields); i += 2 {
		key := fmt.Sprint(fields[i])
		value := "<missing>"
		if i+1 < len(fields) {
			v := fields[i+1]
			if s, ok := v.(string); ok {
				value = fmt.Sprintf("%q", s)
			} else {
				value = fmt.Sprint(v)
			}
		}
		if color {
			parts = append(parts, logCyan+key+logReset+"="+logGreen+value+logReset)
		} else {
			parts = append(parts, key+"="+value)
		}
	}
	log.Printf("%s %s", event, strings.Join(parts, " "))
	if progressLine != "" {
		fmt.Fprintf(os.Stderr, "%s", progressLine)
	}
}

func logColorEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOOKUPX_LOG_COLOR"))) {
	case "always", "true", "1":
		return true
	case "never", "false", "0":
		return false
	}
	if _, disabled := os.LookupEnv("NO_COLOR"); disabled {
		return false
	}
	if term := strings.ToLower(os.Getenv("TERM")); term == "" || term == "dumb" {
		return false
	}
	info, err := os.Stderr.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

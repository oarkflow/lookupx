package pkg

import (
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/oarkflow/sse"
)

// ansiEscape matches ANSI color/control sequences (e.g. "\x1b[36m") that
// LogKV emits when writing to a real terminal. Those are meaningless, and
// render as garbage, on the admin console's log view, so broadcast lines
// have them stripped.
var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*[a-zA-Z]")

var (
	logHub     *sse.Hub
	logHubOnce sync.Once
)

// EnableLogStreaming redirects the standard logger (used by LogKV and every
// log.Printf call in this process) to also broadcast each line over an SSE
// hub, so the admin console can tail server logs live. The hub keeps the
// most recent lines as sticky events, so a client connecting after lines
// were written still sees recent history before the live tail continues.
// Safe to call multiple times; the hub and redirect are installed once.
func EnableLogStreaming() *sse.Hub {
	logHubOnce.Do(func() {
		logHub = sse.NewHub(sse.HubOptions{StickySize: 500, ReplayBufferSize: 500})
		log.SetOutput(io.MultiWriter(os.Stderr, logBroadcastWriter{}))
	})
	return logHub
}

// LogBroadcastWriter returns an io.Writer that broadcasts each written line
// over the log-streaming SSE hub, for wiring into other log sinks (such as
// the HTTP access-log middleware) in addition to the standard logger.
func LogBroadcastWriter() io.Writer { return logBroadcastWriter{} }

type logBroadcastWriter struct{}

func (logBroadcastWriter) Write(p []byte) (int, error) {
	if logHub != nil {
		for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
			line = strings.TrimSpace(ansiEscape.ReplaceAllString(line, ""))
			if line == "" {
				continue
			}
			logHub.Broadcast(sse.NewEvent("log", line).WithTopic("logs"))
		}
	}
	return len(p), nil
}

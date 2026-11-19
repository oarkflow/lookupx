package pkg

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// parseTemporalValue converts a configured temporal value to its sortable
// numeric representation. Date/time values are Unix seconds; time-of-day is
// seconds since midnight.
func parseTemporalValue(raw, format string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty temporal value")
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n, nil
	}
	var layouts []string
	switch strings.ToLower(format) {
	case "date":
		layouts = []string{"2006-01-02", time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05"}
	case "datetime":
		layouts = []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04", "2006-01-02 15:04", time.RFC3339Nano, time.RFC3339}
	case "time":
		for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
			if t, err := time.Parse(layout, raw); err == nil {
				return int64(t.Hour()*3600 + t.Minute()*60 + t.Second()), nil
			}
		}
		return 0, fmt.Errorf("parse time %q: expected HH:MM[:SS]", raw)
	case "datetimez":
		layouts = []string{time.RFC3339Nano, time.RFC3339}
	default:
		layouts = autoTimeLayouts
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Unix(), nil
		}
	}
	if format == "datetimez" {
		return 0, fmt.Errorf("parse datetimez %q: expected RFC3339 with timezone", raw)
	}
	return 0, fmt.Errorf("parse %s %q: invalid temporal value", format, raw)
}

func formatTemporalValue(value int64, format string) string {
	switch strings.ToLower(format) {
	case "date":
		return time.Unix(value, 0).UTC().Format("2006-01-02")
	case "datetime":
		return time.Unix(value, 0).UTC().Format("2006-01-02T15:04:05")
	case "time":
		seconds := ((value % 86400) + 86400) % 86400
		return time.Date(0, 1, 1, int(seconds/3600), int(seconds%3600/60), int(seconds%60), 0, time.UTC).Format("15:04:05")
	default:
		return time.Unix(value, 0).UTC().Format(time.RFC3339)
	}
}

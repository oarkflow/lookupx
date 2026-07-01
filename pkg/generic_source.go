package pkg

import (
	"context"
	"fmt"
	"strconv"
)

// SyntheticField generates a deterministic value for a SourceRecord. It is used
// for examples and apple-to-apple benchmarks without hardcoding any domain such
// as Dataset/Dataset/DatasetC.
type SyntheticField struct {
	Name       string
	Field      FieldID
	Kind       ValueKind
	Values     []string
	NumberBase float64
	NumberMod  uint64
	Normalized bool
}

// StreamingRowsSource emits N generic rows with caller-defined fields and no
// full materialization. It is safe for 1B-row dry runs when combined with
// partitioned builders because only one SourceRecord is reused.
type StreamingRowsSource struct {
	Rows   uint64
	Start  uint64
	Prefix string
	Fields []SyntheticField
}

func (s StreamingRowsSource) Open(ctx context.Context) (Cursor, error) {
	return &streamingRowsCursor{s: s, i: s.Start}, nil
}

type streamingRowsCursor struct {
	s   StreamingRowsSource
	i   uint64
	err error
}

func (c *streamingRowsCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	if c.s.Rows == 0 || c.i >= c.s.Start+c.s.Rows {
		return false
	}
	select {
	case <-ctx.Done():
		c.err = ctx.Err()
		return false
	default:
	}
	seq := c.i + 1
	dst.Reset()
	if c.s.Prefix == "" {
		dst.ID = "row-" + strconv.FormatUint(seq, 10)
	} else {
		dst.ID = fmt.Sprintf("%s-%012d", c.s.Prefix, seq)
	}
	dst.Seq = seq
	for _, f := range c.s.Fields {
		switch f.Kind {
		case ValueKeyword, ValueText:
			v := ""
			if len(f.Values) > 0 {
				v = f.Values[int(seq%uint64(len(f.Values)))]
			}
			if v == "" {
				v = f.Name + "_" + strconv.FormatUint(seq, 10)
			}
			if f.Kind == ValueText {
				dst.AddText(f.Field, v, f.Normalized)
			} else {
				dst.AddKeyword(f.Field, v, f.Normalized)
			}
		case ValueNumber, ValueTimeUnix:
			mod := f.NumberMod
			if mod == 0 {
				mod = c.s.Rows + 1
			}
			dst.AddNumber(f.Field, f.NumberBase+float64(seq%mod))
		}
	}
	c.i++
	return true
}
func (c *streamingRowsCursor) Err() error   { return c.err }
func (c *streamingRowsCursor) Close() error { return nil }

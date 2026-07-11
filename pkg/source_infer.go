package pkg

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// rewindableSample wraps r so its first bytes can be read for sampling and
// then replayed from the start for the real Source to consume. Seekable
// readers (e.g. *os.File) just seek back to 0; anything else is teed into a
// buffer during sampling and replayed via io.MultiReader.
func rewindableSample(r io.Reader) (sample io.Reader, rewind func() (io.Reader, error)) {
	if s, ok := r.(io.Seeker); ok {
		return r, func() (io.Reader, error) {
			if _, err := s.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
			return r, nil
		}
	}
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)
	return tee, func() (io.Reader, error) {
		return io.MultiReader(&buf, r), nil
	}
}

// InferCSVColumns samples up to sampleSize data rows from CSV data (after the
// header row) and proposes an AutoColumn for every header except idColumn. It
// returns a reader that replays the full original CSV stream — including the
// sampled rows — so the caller can still build a working CSVSource from it.
func InferCSVColumns(r io.Reader, idColumn string, sampleSize int) ([]AutoColumn, io.Reader, error) {
	if r == nil {
		return nil, nil, errors.New("nil reader")
	}
	if sampleSize <= 0 {
		sampleSize = 200
	}
	sampleR, rewind := rewindableSample(r)
	cr := csv.NewReader(sampleR)
	header, err := cr.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("read csv header: %w", err)
	}
	samples := make([][]string, len(header))
	n := 0
	for n < sampleSize {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		for i, v := range row {
			if i < len(samples) && v != "" && len(samples[i]) < 25 {
				samples[i] = append(samples[i], v)
			}
		}
		n++
	}
	rewound, err := rewind()
	if err != nil {
		return nil, nil, err
	}

	idLower := strings.ToLower(strings.TrimSpace(idColumn))
	out := make([]AutoColumn, 0, len(header))
	for i, h := range header {
		if strings.ToLower(h) == idLower {
			continue
		}
		out = append(out, classifyFromValues(h, samples[i]))
	}
	return out, rewound, nil
}

// AutoCSVSource infers field kinds and schema by sampling CSV data, creates a
// new Index (merging the inferred schema under any fields cfg.Schema already
// declares explicitly), and returns the Index alongside a ready-to-use
// CSVSource — no manual Schema.Fields map, FieldID() calls, or Bindings slice
// required.
func AutoCSVSource(ctx context.Context, cfg Config, r io.Reader, idColumn string) (*Index, CSVSource, error) {
	cols, rewound, err := InferCSVColumns(r, idColumn, 200)
	if err != nil {
		return nil, CSVSource{}, err
	}
	ix, err := New(mergeAutoSchema(cfg, cols))
	if err != nil {
		return nil, CSVSource{}, err
	}
	bindings := make([]CSVBinding, len(cols))
	for i, c := range cols {
		bindings[i] = CSVBinding{Column: c.Column, Field: ix.FieldID(c.Field), Kind: c.Kind, Layout: c.Layout}
	}
	return ix, CSVSource{R: rewound, IDColumn: idColumn, Bindings: bindings}, nil
}

// InferJSONLColumns samples up to sampleSize lines of newline-delimited JSON
// and proposes an AutoColumn for every observed object key except idField.
// Keys are visited in first-seen order for deterministic output. It returns a
// reader that replays the full original stream, including sampled lines.
func InferJSONLColumns(r io.Reader, idField string, sampleSize int) ([]AutoColumn, io.Reader, error) {
	if r == nil {
		return nil, nil, errors.New("nil reader")
	}
	if sampleSize <= 0 {
		sampleSize = 200
	}
	sampleR, rewind := rewindableSample(r)
	sc := bufio.NewScanner(sampleR)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	valueSamples := map[string][]string{}
	seen := map[string]bool{}
	order := make([]string, 0, 16)
	n := 0
	for n < sampleSize && sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, nil, fmt.Errorf("parse jsonl line: %w", err)
		}
		for k, v := range m {
			if v == nil {
				continue
			}
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
			if len(valueSamples[k]) < 25 {
				valueSamples[k] = append(valueSamples[k], fmt.Sprint(v))
			}
		}
		n++
	}
	if err := sc.Err(); err != nil {
		return nil, nil, err
	}
	rewound, err := rewind()
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(order)

	idLower := strings.ToLower(strings.TrimSpace(idField))
	out := make([]AutoColumn, 0, len(order))
	for _, k := range order {
		if strings.ToLower(k) == idLower {
			continue
		}
		out = append(out, classifyFromValues(k, valueSamples[k]))
	}
	return out, rewound, nil
}

// AutoJSONLSource infers field kinds and schema by sampling newline-delimited
// JSON data, creates a new Index (merging the inferred schema under any
// fields cfg.Schema already declares explicitly), and returns the Index
// alongside a ready-to-use JSONLSource.
func AutoJSONLSource(ctx context.Context, cfg Config, r io.Reader, idField string) (*Index, JSONLSource, error) {
	cols, rewound, err := InferJSONLColumns(r, idField, 200)
	if err != nil {
		return nil, JSONLSource{}, err
	}
	ix, err := New(mergeAutoSchema(cfg, cols))
	if err != nil {
		return nil, JSONLSource{}, err
	}
	bindings := make([]JSONBinding, len(cols))
	for i, c := range cols {
		bindings[i] = JSONBinding{FieldName: c.Field, Field: ix.FieldID(c.Field), Kind: c.Kind, Layout: c.Layout}
	}
	return ix, JSONLSource{R: rewound, IDField: idField, Bindings: bindings}, nil
}

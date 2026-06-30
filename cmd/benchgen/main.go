package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

type row struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}

type query struct {
	Name   string            `json:"name"`
	Fields map[string]string `json:"fields"`
	Limit  int               `json:"limit"`
}

func main() {
	rows := flag.Int("rows", 100000, "number of rows")
	out := flag.String("out", "benchdata", "output directory")
	fieldsCSV := flag.String("fields", "field_a,field_b,field_c", "comma-separated indexed fields")
	flag.Parse()
	if err := os.MkdirAll(*out, 0755); err != nil {
		panic(err)
	}
	fields := split(*fieldsCSV)
	dataPath := *out + "/dataset.jsonl"
	queryPath := *out + "/queries.jsonl"
	data, err := os.Create(dataPath)
	if err != nil {
		panic(err)
	}
	defer data.Close()
	w := bufio.NewWriterSize(data, 1<<20)
	for i := 0; i < *rows; i++ {
		m := map[string]string{}
		for pos, f := range fields {
			m[f] = generatedValue(f, pos, i)
		}
	b, _ := json.Marshal(row{ID: fmt.Sprintf("doc-%09d", i), Fields: m})
		w.Write(b)
		w.WriteByte('\n')
	}
	w.Flush()
	qf, err := os.Create(queryPath)
	if err != nil {
		panic(err)
	}
	defer qf.Close()
	qw := bufio.NewWriter(qf)
	// choose values that exist with the generated distributions for any field names.
	// Row 0 is deterministic and does not rely on business/domain-specific names.
	first := map[string]string{}
	for pos, f := range fields {
		first[f] = generatedValue(f, pos, 0)
	}
	for _, q := range []query{
		{Name: "exact-composite", Fields: pick(fields, first), Limit: 10},
	} {
		b, _ := json.Marshal(q)
		qw.Write(b)
		qw.WriteByte('\n')
	}
	qw.Flush()
	fmt.Println(dataPath)
	fmt.Println(queryPath)
}

func generatedValue(field string, pos int, row int) string {
	switch pos % 3 {
	case 0:
		values := [...]string{"alpha", "beta", "gamma", "delta", "omega"}
		return values[row%len(values)]
	case 1:
		return fmt.Sprintf("%d", row%16)
	default:
		return fmt.Sprintf("2026-01-%02d", (row%28)+1)
	}
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func pick(fields []string, vals map[string]string) map[string]string {
	m := map[string]string{}
	for _, f := range fields {
		if v := vals[f]; v != "" {
			m[f] = v
		}
	}
	return m
}

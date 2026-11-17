package pkg

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

const maxIndexUploadBytes = 1 << 30 // 1 GiB

type filePathSource func(context.Context) (Cursor, error)

func (source filePathSource) Open(ctx context.Context) (Cursor, error) {
	return source(ctx)
}

// reloadFile accepts multipart file uploads and streams CSV, TSV, JSON,
// JSON Lines, and XLSX rows into a replacement index generation. Tabular
// formats use their first row as column headers; XLSX uses the selected sheet
// or the workbook's first sheet.
func (s *MultiServer) reloadFile(ctx context.Context, id string, w http.ResponseWriter, r *http.Request) (BulkStats, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxIndexUploadBytes+(64<<20))
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		return BulkStats{}, fmt.Errorf("read upload form: %w", err)
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		return BulkStats{}, fmt.Errorf("file is required: %w", err)
	}
	defer file.Close()
	if header.Size > maxIndexUploadBytes {
		return BulkStats{}, fmt.Errorf("file exceeds the %d MiB upload limit", maxIndexUploadBytes>>20)
	}

	format := strings.ToLower(strings.TrimSpace(r.FormValue("format")))
	if format == "" || format == "auto" {
		format = strings.TrimPrefix(strings.ToLower(filepath.Ext(header.Filename)), ".")
	}
	idField := strings.TrimSpace(r.FormValue("id_field"))

	upload, err := os.CreateTemp("", "lookupx-upload-*")
	if err != nil {
		return BulkStats{}, fmt.Errorf("create upload file: %w", err)
	}
	uploadPath := upload.Name()
	defer os.Remove(uploadPath)
	written, copyErr := io.Copy(upload, io.LimitReader(file, maxIndexUploadBytes+1))
	closeErr := upload.Close()
	if copyErr != nil {
		return BulkStats{}, fmt.Errorf("save uploaded file: %w", copyErr)
	}
	if closeErr != nil {
		return BulkStats{}, fmt.Errorf("close uploaded file: %w", closeErr)
	}
	if written > maxIndexUploadBytes {
		return BulkStats{}, fmt.Errorf("file exceeds the %d MiB upload limit", maxIndexUploadBytes>>20)
	}

	dataPath := uploadPath
	effectiveFormat := format
	var convertedPath string
	switch format {
	case "csv", "jsonl", "ndjson":
		if format == "ndjson" {
			effectiveFormat = "jsonl"
		}
	case "tsv":
		convertedPath, err = convertDelimitedToCSV(uploadPath, '\t')
		effectiveFormat = "csv"
	case "json":
		convertedPath, err = convertJSONToJSONL(uploadPath)
		effectiveFormat = "jsonl"
	case "xlsx":
		convertedPath, err = convertXLSXToCSV(uploadPath, strings.TrimSpace(r.FormValue("sheet")))
		effectiveFormat = "csv"
	default:
		return BulkStats{}, fmt.Errorf("unsupported file format %q; use CSV, TSV, XLSX, JSON, or JSON Lines", format)
	}
	if err != nil {
		return BulkStats{}, err
	}
	if convertedPath != "" {
		dataPath = convertedPath
		defer os.Remove(convertedPath)
	}

	input, err := os.Open(dataPath)
	if err != nil {
		return BulkStats{}, err
	}
	var columns []AutoColumn
	switch effectiveFormat {
	case "csv":
		columns, _, err = InferCSVColumns(input, idField, 200)
	case "jsonl":
		columns, _, err = InferJSONLColumns(input, idField, 200)
	}
	closeErr = input.Close()
	if err != nil {
		return BulkStats{}, fmt.Errorf("inspect file columns: %w", err)
	}
	if closeErr != nil {
		return BulkStats{}, closeErr
	}
	managed, ok := s.Manager.Managed(id)
	cfg := Config{}
	if ok {
		cfg = managed.Config
	}
	cfg.Schema = AutoSchema(columns)
	cfg.DisableSource = false
	if _, _, err := s.Manager.EnsureIndex(id, cfg); err != nil {
		return BulkStats{}, err
	}
	batchSize := 0
	if raw := strings.TrimSpace(r.FormValue("batch_size")); raw != "" {
		batchSize, _ = strconv.Atoi(raw)
	}
	options := bulkFromSQLReq(id, batchSize, "", false)

	return s.Manager.ReloadWithConfig(ctx, id, cfg, func(ix *Index) (Source, error) {
		if effectiveFormat == "csv" {
			bindings := make([]CSVBinding, len(columns))
			for i, col := range columns {
				bindings[i] = CSVBinding{Column: col.Column, Field: ix.FieldID(col.Field), Kind: col.Kind, Layout: col.Layout}
			}
			return filePathSource(func(ctx context.Context) (Cursor, error) {
				f, err := os.Open(dataPath)
				if err != nil {
					return nil, err
				}
				cur, err := (CSVSource{R: f, IDColumn: idField, Bindings: bindings}).Open(ctx)
				if err != nil {
					f.Close()
					return nil, err
				}
				return &fileClosingCursor{cur: cur, f: f}, nil
			}), nil
		}
		bindings := make([]JSONBinding, len(columns))
		for i, col := range columns {
			bindings[i] = JSONBinding{FieldName: col.Field, Field: ix.FieldID(col.Field), Kind: col.Kind, Layout: col.Layout}
		}
		return filePathSource(func(ctx context.Context) (Cursor, error) {
			f, err := os.Open(dataPath)
			if err != nil {
				return nil, err
			}
			cur, err := (JSONLSource{R: f, IDField: idField, Bindings: bindings}).Open(ctx)
			if err != nil {
				f.Close()
				return nil, err
			}
			return &fileClosingCursor{cur: cur, f: f}, nil
		}), nil
	}, options)
}

func convertJSONToJSONL(inputPath string) (string, error) {
	in, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer in.Close()
	out, err := os.CreateTemp("", "lookupx-jsonl-*.jsonl")
	if err != nil {
		return "", err
	}
	path := out.Name()
	cleanup := func(err error) (string, error) {
		out.Close()
		os.Remove(path)
		return "", err
	}

	br := bufio.NewReader(in)
	var first byte
	for {
		first, err = br.ReadByte()
		if err != nil {
			return cleanup(fmt.Errorf("read JSON file: %w", err))
		}
		if first != ' ' && first != '\t' && first != '\r' && first != '\n' {
			if err := br.UnreadByte(); err != nil {
				return cleanup(err)
			}
			break
		}
	}
	dec := json.NewDecoder(br)
	writeRecord := func(record map[string]any) error {
		if record == nil {
			return errors.New("JSON file must contain objects")
		}
		b, err := json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = out.Write(append(b, '\n'))
		return err
	}
	if first == '[' {
		tok, err := dec.Token()
		if err != nil {
			return cleanup(fmt.Errorf("read JSON array: %w", err))
		}
		if tok != json.Delim('[') {
			return cleanup(errors.New("JSON file must contain an object or array of objects"))
		}
		for dec.More() {
			var record map[string]any
			if err := dec.Decode(&record); err != nil {
				return cleanup(fmt.Errorf("decode JSON array record: %w", err))
			}
			if err := writeRecord(record); err != nil {
				return cleanup(err)
			}
		}
		if _, err := dec.Token(); err != nil {
			return cleanup(fmt.Errorf("close JSON array: %w", err))
		}
	} else {
		var record map[string]any
		if err := dec.Decode(&record); err != nil {
			return cleanup(fmt.Errorf("decode JSON object: %w", err))
		}
		if err := writeRecord(record); err != nil {
			return cleanup(err)
		}
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple top-level JSON values are not supported; use JSON Lines")
		}
		return cleanup(err)
	}
	if err := out.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func convertDelimitedToCSV(inputPath string, delimiter rune) (string, error) {
	in, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer in.Close()
	reader := csv.NewReader(in)
	reader.Comma = delimiter
	out, err := os.CreateTemp("", "lookupx-csv-*.csv")
	if err != nil {
		return "", err
	}
	path := out.Name()
	writer := csv.NewWriter(out)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			out.Close()
			os.Remove(path)
			return "", fmt.Errorf("read delimited file: %w", err)
		}
		if err := writer.Write(row); err != nil {
			out.Close()
			os.Remove(path)
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		out.Close()
		os.Remove(path)
		return "", err
	}
	if err := out.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func convertXLSXToCSV(inputPath, sheetName string) (string, error) {
	book, err := excelize.OpenFile(inputPath)
	if err != nil {
		return "", fmt.Errorf("open XLSX workbook: %w", err)
	}
	defer book.Close()

	if sheetName == "" {
		sheets := book.GetSheetList()
		if len(sheets) == 0 {
			return "", errors.New("XLSX workbook has no worksheets")
		}
		sheetName = sheets[0]
	}
	rows, err := book.Rows(sheetName)
	if err != nil {
		return "", fmt.Errorf("open worksheet %q: %w", sheetName, err)
	}
	defer rows.Close()

	out, err := os.CreateTemp("", "lookupx-xlsx-*.csv")
	if err != nil {
		return "", err
	}
	path := out.Name()
	writer := csv.NewWriter(out)
	for rows.Next() {
		values, err := rows.Columns()
		if err != nil {
			out.Close()
			os.Remove(path)
			return "", fmt.Errorf("read worksheet row: %w", err)
		}
		if err := writer.Write(values); err != nil {
			out.Close()
			os.Remove(path)
			return "", err
		}
	}
	if err := rows.Error(); err != nil {
		out.Close()
		os.Remove(path)
		return "", err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		out.Close()
		os.Remove(path)
		return "", err
	}
	if err := out.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

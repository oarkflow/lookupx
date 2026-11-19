package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/fh"
)

type adminIndexSummary struct {
	ID           string `json:"id"`
	DatasourceID string `json:"datasource_id,omitempty"`
	Kind         string `json:"kind,omitempty"`
	Driver       string `json:"driver,omitempty"`
}

type adminIndexEditor struct {
	ID               string `json:"id"`
	DatasourceID     string `json:"datasource_id"`
	IndexBlock       string `json:"index_block"`
	Driver           string `json:"driver"`
	Database         string `json:"database_expression"`
	DatabaseEditable bool   `json:"database_editable"`
	SQLQuery         string `json:"sql_query"`
	SQLQueryEditable bool   `json:"sql_query_editable"`
}

type updateAdminIndexEditorRequest struct {
	IndexBlock string `json:"index_block"`
	Driver     string `json:"driver"`
	Database   string `json:"database_expression"`
	SQLQuery   string `json:"sql_query"`
	UpdateSQL  bool   `json:"update_sql"`
}

type sourceEdit struct {
	start, end int
	replace    string
}

var safeSQLDriver = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_+-]{0,31}$`)

func (s *MultiServer) listAdminIndexes(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, false); err != nil {
		return err
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	_, cfg, _, err := s.readAdminBCL()
	if err != nil {
		return fhError(c, 500, "could not parse BCL configuration: "+err.Error())
	}
	datasources := make(map[string]DatasourceDef, len(cfg.Datasources))
	for _, ds := range cfg.Datasources {
		datasources[ds.ID] = ds
	}
	out := make([]adminIndexSummary, 0, len(cfg.Indexes))
	for _, idx := range cfg.Indexes {
		item := adminIndexSummary{ID: idx.ID, DatasourceID: idx.DatasourceID}
		if ds, ok := datasources[idx.DatasourceID]; ok {
			item.Kind, item.Driver = ds.Kind, ds.Driver
		}
		out = append(out, item)
	}
	return c.JSON(out)
}

func (s *MultiServer) getAdminIndexEditor(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, false); err != nil {
		return err
	}
	id := c.Param("id")
	s.configMu.Lock()
	defer s.configMu.Unlock()
	source, cfg, doc, err := s.readAdminBCL()
	if err != nil {
		return fhError(c, 500, "could not parse BCL configuration: "+err.Error())
	}
	idx, ok := findIndexDef(cfg, id)
	if !ok {
		return fhError(c, 404, "index not found in BCL configuration")
	}
	indexBlock, ok := findTopLevelBCLBlock(doc, "index", id)
	if !ok {
		return fhError(c, 500, "index block not found in BCL source")
	}
	start, end, err := bclBlockBounds(string(source), indexBlock)
	if err != nil {
		return fhError(c, 500, "could not locate index block in BCL source")
	}
	result := adminIndexEditor{ID: idx.ID, DatasourceID: idx.DatasourceID, IndexBlock: string(source[start:end])}
	if idx.DatasourceID != "" {
		dsDef, exists := findDatasourceDef(cfg, idx.DatasourceID)
		if !exists {
			return fhError(c, 500, "index datasource not found in BCL configuration")
		}
		result.Driver = dsDef.Driver
		dsBlock, exists := findTopLevelBCLBlock(doc, "datasource", idx.DatasourceID)
		if !exists {
			return fhError(c, 500, "datasource block not found in BCL source")
		}
		if assignment := findBCLAssignment(dsBlock, "dsn"); assignment != nil {
			result.Database = rawBCLValue(source, assignment.Value)
		}
		result.DatabaseEditable = findBCLAssignment(dsBlock, "driver") != nil && findBCLAssignment(dsBlock, "dsn") != nil
		if assignment := findBCLAssignment(dsBlock, "query"); assignment != nil {
			if literal, ok := assignment.Value.(*bcl.Literal); ok && literal.Type == "string" {
				result.SQLQuery, _ = literal.Data.(string)
				result.SQLQueryEditable = true
			}
		}
	}
	c.Set("Cache-Control", "no-store")
	return c.JSON(result)
}

func (s *MultiServer) updateAdminIndexEditor(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, true); err != nil {
		return err
	}
	id := c.Param("id")
	var req updateAdminIndexEditorRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid index configuration")
	}
	if len(req.IndexBlock) > 1<<20 || len(req.Database) > 16<<10 || len(req.SQLQuery) > 1<<20 {
		return fhError(c, 413, "index configuration editor content is too large")
	}
	if strings.TrimSpace(req.IndexBlock) == "" {
		return fhError(c, 400, "index configuration is required")
	}

	s.configMu.Lock()
	defer s.configMu.Unlock()
	source, cfg, doc, err := s.readAdminBCL()
	if err != nil {
		return fhError(c, 500, "could not read BCL configuration")
	}
	idx, ok := findIndexDef(cfg, id)
	if !ok {
		return fhError(c, 404, "index not found in BCL configuration")
	}
	indexBlock, ok := findTopLevelBCLBlock(doc, "index", id)
	if !ok {
		return fhError(c, 500, "index block not found in BCL source")
	}
	indexStart, indexEnd, err := bclBlockBounds(string(source), indexBlock)
	if err != nil {
		return fhError(c, 500, "could not locate index block in BCL source")
	}
	edits := []sourceEdit{{start: indexStart, end: indexEnd, replace: strings.TrimSpace(req.IndexBlock)}}
	if idx.DatasourceID != "" {
		dsBlock, exists := findTopLevelBCLBlock(doc, "datasource", idx.DatasourceID)
		if !exists {
			return fhError(c, 500, "datasource block not found in BCL source")
		}
		driverAssignment := findBCLAssignment(dsBlock, "driver")
		dsnAssignment := findBCLAssignment(dsBlock, "dsn")
		if driverAssignment != nil && dsnAssignment != nil {
			req.Driver, req.Database = strings.TrimSpace(req.Driver), strings.TrimSpace(req.Database)
			if !safeSQLDriver.MatchString(req.Driver) {
				return fhError(c, 400, "database driver is invalid")
			}
			if req.Database == "" || strings.ContainsAny(req.Database, "\r\n") {
				return fhError(c, 400, "database DSN expression is required on one line")
			}
			if err := addBCLValueEdit(&edits, driverAssignment.Value, req.Driver); err != nil {
				return fhError(c, 500, "could not locate datasource driver in BCL source")
			}
			if err := addBCLValueEdit(&edits, dsnAssignment.Value, req.Database); err != nil {
				return fhError(c, 500, "could not locate datasource DSN in BCL source")
			}
		}
		queryAssignment := findBCLAssignment(dsBlock, "query")
		if req.UpdateSQL && queryAssignment != nil {
			if !req.SQLQueryEditable(queryAssignment) {
				return fhError(c, 400, "datasource query is not a string literal and cannot be edited here")
			}
			if err := addBCLValueEdit(&edits, queryAssignment.Value, quoteBCLString(req.SQLQuery)); err != nil {
				return fhError(c, 500, "could not locate datasource SQL query in BCL source")
			}
		} else if req.UpdateSQL && strings.TrimSpace(req.SQLQuery) != "" {
			return fhError(c, 400, "datasource does not define an editable SQL query")
		}
	} else if req.UpdateSQL && strings.TrimSpace(req.SQLQuery) != "" {
		return fhError(c, 400, "index does not have a datasource query to edit")
	}
	updated, err := applySourceEdits(source, edits)
	if err != nil {
		return fhError(c, 500, "could not update BCL configuration")
	}
	updatedCfg, err := ParseBCLBytes(updated, nil)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	updatedIndex, ok := findIndexDef(updatedCfg, id)
	if !ok || updatedIndex.DatasourceID != idx.DatasourceID {
		return fhError(c, 400, "index ID and datasource assignment must remain unchanged in this editor")
	}
	if err := validateBCLContentAt(string(updated), filepath.Dir(s.ConfigPath)); err != nil {
		return fhError(c, 400, err.Error())
	}
	if err := writePrivateAtomic(s.ConfigPath, updated); err != nil {
		return fhError(c, 500, "could not save index configuration")
	}
	return c.JSON(map[string]any{"saved": true, "restart_required": true})
}

func (req updateAdminIndexEditorRequest) SQLQueryEditable(assignment *bcl.Assignment) bool {
	literal, ok := assignment.Value.(*bcl.Literal)
	return ok && literal.Type == "string"
}

func (s *MultiServer) readAdminBCL() ([]byte, *LookupXConfig, *bcl.Document, error) {
	if strings.TrimSpace(s.ConfigPath) == "" {
		return nil, nil, nil, fmt.Errorf("BCL configuration editing is disabled")
	}
	source, err := os.ReadFile(s.ConfigPath)
	if err != nil {
		return nil, nil, nil, err
	}
	cfg, err := ParseBCLBytes(source, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	cfg.BaseDir, err = filepath.Abs(filepath.Dir(s.ConfigPath))
	if err != nil {
		return nil, nil, nil, err
	}
	doc, err := bcl.Parse(source)
	if err != nil {
		return nil, nil, nil, err
	}
	return source, cfg, doc, nil
}

func findIndexDef(cfg *LookupXConfig, id string) (IndexDef, bool) {
	for _, item := range cfg.Indexes {
		if item.ID == id {
			return item, true
		}
	}
	return IndexDef{}, false
}

func findDatasourceDef(cfg *LookupXConfig, id string) (DatasourceDef, bool) {
	for _, item := range cfg.Datasources {
		if item.ID == id {
			return item, true
		}
	}
	return DatasourceDef{}, false
}

func findTopLevelBCLBlock(doc *bcl.Document, kind, id string) (*bcl.Block, bool) {
	for _, node := range doc.Items {
		block, ok := node.(*bcl.Block)
		if ok && strings.EqualFold(block.Type, kind) && block.ID == id {
			return block, true
		}
	}
	return nil, false
}

func findBCLAssignment(block *bcl.Block, name string) *bcl.Assignment {
	for _, node := range block.Body {
		assignment, ok := node.(*bcl.Assignment)
		if ok && strings.EqualFold(assignment.Name, name) {
			return assignment
		}
	}
	return nil
}

func rawBCLValue(source []byte, value bcl.Value) string {
	if value == nil {
		return ""
	}
	span := value.GetSpan()
	if span.Start.Offset < 0 || span.End.Offset > len(source) || span.Start.Offset >= span.End.Offset {
		return ""
	}
	return strings.TrimSpace(string(source[span.Start.Offset:span.End.Offset]))
}

func addBCLValueEdit(edits *[]sourceEdit, value bcl.Value, replacement string) error {
	if value == nil {
		return fmt.Errorf("missing BCL value")
	}
	span := value.GetSpan()
	if span.Start.Offset < 0 || span.End.Offset <= span.Start.Offset {
		return fmt.Errorf("invalid BCL value span")
	}
	*edits = append(*edits, sourceEdit{start: span.Start.Offset, end: span.End.Offset, replace: replacement})
	return nil
}

func applySourceEdits(source []byte, edits []sourceEdit) ([]byte, error) {
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	updated := append([]byte(nil), source...)
	lastStart := len(source) + 1
	for _, edit := range edits {
		if edit.start < 0 || edit.end < edit.start || edit.end > len(updated) || edit.end > lastStart {
			return nil, fmt.Errorf("overlapping or invalid source edits")
		}
		updated = append(updated[:edit.start], append([]byte(edit.replace), updated[edit.end:]...)...)
		lastStart = edit.start
	}
	return updated, nil
}

func bclBlockBounds(source string, block *bcl.Block) (int, int, error) {
	start := block.Span.Start.Offset
	if start < 0 || start >= len(source) {
		return 0, 0, fmt.Errorf("invalid block span")
	}
	depth, open := 0, false
	for i := start; i < len(source); {
		if strings.HasPrefix(source[i:], "//") || source[i] == '#' {
			for i < len(source) && source[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(source[i:], "/*") {
			end := strings.Index(source[i+2:], "*/")
			if end < 0 {
				return 0, 0, fmt.Errorf("unterminated BCL comment")
			}
			i += end + 4
			continue
		}
		if strings.HasPrefix(source[i:], `"""`) {
			end := strings.Index(source[i+3:], `"""`)
			if end < 0 {
				return 0, 0, fmt.Errorf("unterminated BCL multiline string")
			}
			i += end + 6
			continue
		}
		if source[i] == '"' || source[i] == '\'' || source[i] == '`' {
			quote := source[i]
			i++
			for i < len(source) {
				if quote != '`' && source[i] == '\\' && i+1 < len(source) {
					i += 2
					continue
				}
				if source[i] == quote {
					i++
					break
				}
				i++
			}
			continue
		}
		if open && strings.HasPrefix(source[i:], "<<") {
			lineEnd := strings.IndexByte(source[i:], '\n')
			if lineEnd > 0 {
				marker := strings.TrimSpace(source[i+2 : i+lineEnd])
				if marker != "" {
					needle := "\n" + marker
					end := strings.Index(source[i+lineEnd:], needle)
					if end >= 0 {
						i += lineEnd + end + len(needle)
						continue
					}
				}
			}
		}
		switch source[i] {
		case '{':
			open = true
			depth++
		case '}':
			if open {
				depth--
				if depth == 0 {
					return start, i + 1, nil
				}
			}
		}
		i++
	}
	return 0, 0, fmt.Errorf("unclosed BCL block")
}

func quoteBCLString(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

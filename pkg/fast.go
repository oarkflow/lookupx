package pkg

import "errors"

type FieldID uint16

const InvalidFieldID FieldID = ^FieldID(0)

func (ix *Index) FieldID(name string) FieldID {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if id, ok := ix.fieldIDs[name]; ok {
		return id
	}
	return InvalidFieldID
}

func (ix *Index) fieldByID(id FieldID) *schemaField {
	if int(id) >= len(ix.fieldList) {
		return nil
	}
	return &ix.fieldList[id]
}

func (ix *Index) elapsed(start int64) int64 {
	if start == 0 {
		return 0
	}
	return ix.clock.UnixNano() - start
}

type RowWriter struct {
	ix  *Index
	did DocID
	err error
}

func (ix *Index) UpsertFast(id string, fn func(*RowWriter)) error {
	if id == "" {
		return errors.New("id required")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	did := ix.reserveDocLocked(id)
	w := RowWriter{ix: ix, did: did}
	fn(&w)
	return w.err
}

func (ix *Index) reserveDocLocked(id string) DocID {
	if old, ok := ix.extToDoc[id]; ok {
		ix.deleted.Add(old)
		ix.live.Remove(old)
		ix.hasDeletes = true
	}
	did := ix.nextDocID
	ix.nextDocID++
	ix.extToDoc[id] = did
	ix.live.Add(did)
	ix.docToExt = append(ix.docToExt, id)
	ix.docs = append(ix.docs, nil)
	return did
}

func (w *RowWriter) Keyword(fid FieldID, value string)           { w.keyword(fid, value, false) }
func (w *RowWriter) KeywordNormalized(fid FieldID, value string) { w.keyword(fid, value, true) }
func (w *RowWriter) Text(fid FieldID, value string)              { w.text(fid, value, false) }
func (w *RowWriter) TextNormalized(fid FieldID, value string)    { w.text(fid, value, true) }
func (w *RowWriter) Int(fid FieldID, value int64)                { w.number(fid, float64(value)) }
func (w *RowWriter) Float(fid FieldID, value float64)            { w.number(fid, value) }

func (w *RowWriter) keyword(fid FieldID, value string, normalized bool) {
	if w.err != nil || value == "" {
		return
	}
	sf := w.ix.fieldByID(fid)
	if sf == nil {
		return
	}
	v := value
	if !normalized {
		v = normalizeStringByKind(sf.special, value, sf.opt.Lowercase)
	}
	if v == "" {
		return
	}
	sf.fi.exists.Add(w.did)
	if col := w.ix.strings[sf.name]; col != nil {
		col[w.did] = v
	}
	if sf.special == specialIP {
		if ip, ok := parseIPv4(v); ok {
			if w.ix.ip4[sf.name] == nil {
				w.ix.ip4[sf.name] = make(map[DocID]uint32, w.ix.cfg.InitialCapacity)
			}
			w.ix.ip4[sf.name][w.did] = ip
			w.ix.ip4Dirty[sf.name] = true
		}
	}
	if sf.opt.Unique {
		if old, exists := sf.fi.unique[v]; exists && !w.ix.deleted.Has(old) && old != w.did {
			w.err = errors.New("unique constraint violation")
			return
		}
		sf.fi.unique[v] = w.did
	} else {
		addTerm(sf.fi, v, w.did, sf.opt.Fuzzy)
	}
	w.ix.indexDerivedLocked(sf.fi, sf.opt, v, w.did)
}

func (w *RowWriter) text(fid FieldID, value string, normalized bool) {
	if w.err != nil || value == "" {
		return
	}
	sf := w.ix.fieldByID(fid)
	if sf == nil {
		return
	}
	v := value
	if !normalized {
		v = normalizeStringByKind(sf.special, value, sf.opt.Lowercase)
	}
	if v == "" {
		return
	}
	sf.fi.exists.Add(w.did)
	if col := w.ix.strings[sf.name]; col != nil {
		col[w.did] = v
	}
	w.ix.tokens = analyzeWith(sf.opt, v, w.ix.tokens)
	for pos, t := range w.ix.tokens {
		addTerm(sf.fi, t, w.did, sf.opt.Fuzzy)
		if sf.opt.Phrase {
			addPos(sf.fi, t, w.did, uint32(pos))
		}
		w.ix.indexDerivedLocked(sf.fi, sf.opt, t, w.did)
	}
	if sf.opt.Phrase {
		addPhrases(sf.fi, w.ix.tokens, w.did)
	}
}

func (w *RowWriter) number(fid FieldID, value float64) {
	if w.err != nil {
		return
	}
	sf := w.ix.fieldByID(fid)
	if sf == nil {
		return
	}
	sf.fi.exists.Add(w.did)
	w.ix.setNumericLocked(sf.name, w.did, value)
	if sf.opt.TTLField {
		w.ix.expires[w.did] = int64(value)
	}
}

func (ix *Index) setNumericLocked(field string, did DocID, value float64) {
	if col := ix.numeric[field]; col != nil {
		col[did] = value
	}
	vals := ix.numericDense[field]
	if int(did) >= len(vals) {
		n := int(did) + 1
		if n < len(vals)*2 {
			n = len(vals) * 2
		}
		if n == 0 {
			n = 1
		}
		nv := make([]float64, n)
		copy(nv, vals)
		vals = nv
		ix.numericDense[field] = vals
	}
	vals[did] = value
	if ex := ix.numericExists[field]; ex != nil {
		ex.Add(did)
	}
	ix.numDirty[field] = true
}

func (ix *Index) BatchUpsertFast(ids []string, build func(i int, w *RowWriter)) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for i, id := range ids {
		if id == "" {
			return errors.New("id required")
		}
		did := ix.reserveDocLocked(id)
		w := RowWriter{ix: ix, did: did}
		build(i, &w)
		if w.err != nil {
			return w.err
		}
	}
	return nil
}

func (ix *Index) SearchTermFast(fid FieldID, normalizedValue string, limit int, dst []Hit) []Hit {
	dst = dst[:0]
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf := ix.fieldByID(fid)
	if sf == nil {
		return dst
	}
	fi := sf.fi
	if did, ok := fi.unique[normalizedValue]; ok && !ix.isDeletedOrExpiredLocked(did) {
		return append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
	}
	if did := fi.termOne[normalizedValue]; did != 0 && !ix.isDeletedOrExpiredLocked(did) {
		return append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
	}
	if b := fi.terms[normalizedValue]; b != nil {
		b.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
			return limit <= 0 || len(dst) < limit
		})
	}
	return dst
}

func (ix *Index) CountTermFast(fid FieldID, normalizedValue string) int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf := ix.fieldByID(fid)
	if sf == nil {
		return 0
	}
	fi := sf.fi
	if did, ok := fi.unique[normalizedValue]; ok && !ix.isDeletedOrExpiredLocked(did) {
		return 1
	}
	if did := fi.termOne[normalizedValue]; did != 0 && !ix.isDeletedOrExpiredLocked(did) {
		return 1
	}
	return ix.countLiveLocked(fi.terms[normalizedValue])
}

// BeginFast starts a zero-allocation typed row upsert. The caller must call Commit.
func (ix *Index) BeginFast(id string) RowWriter {
	ix.mu.Lock()
	did := ix.reserveDocLocked(id)
	return RowWriter{ix: ix, did: did}
}

func (w *RowWriter) Commit() error {
	if w.ix != nil {
		w.ix.mu.Unlock()
		w.ix = nil
	}
	return w.err
}

type BatchWriter struct {
	ix  *Index
	err error
}

func (ix *Index) BeginBatchFast() BatchWriter { ix.mu.Lock(); return BatchWriter{ix: ix} }
func (bw *BatchWriter) Begin(id string) RowWriter {
	if bw.err != nil || bw.ix == nil {
		return RowWriter{err: bw.err}
	}
	if id == "" {
		bw.err = errors.New("id required")
		return RowWriter{err: bw.err}
	}
	did := bw.ix.reserveDocLocked(id)
	return RowWriter{ix: bw.ix, did: did}
}
func (bw *BatchWriter) Commit() error {
	if bw.ix != nil {
		bw.ix.mu.Unlock()
		bw.ix = nil
	}
	return bw.err
}

type KeywordField struct {
	sf  *schemaField
	col map[DocID]string
}
type NumericField struct {
	name   string
	sf     *schemaField
	vals   []float64
	exists *Bitmap
}

func (ix *Index) KeywordField(name string) KeywordField {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if id, ok := ix.fieldIDs[name]; ok {
		sf := &ix.fieldList[id]
		return KeywordField{sf: sf, col: ix.strings[name]}
	}
	return KeywordField{}
}
func (ix *Index) NumericField(name string) NumericField {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if id, ok := ix.fieldIDs[name]; ok {
		sf := &ix.fieldList[id]
		return NumericField{name: name, sf: sf, vals: ix.numericDense[name], exists: ix.numericExists[name]}
	}
	return NumericField{}
}

func (w *RowWriter) KeywordH(h KeywordField, value string)           { w.keywordH(h, value, false) }
func (w *RowWriter) KeywordHNormalized(h KeywordField, value string) { w.keywordH(h, value, true) }
func (w *RowWriter) TextHNormalized(h KeywordField, value string)    { w.textH(h, value, true) }
func (w *RowWriter) FloatH(h NumericField, value float64)            { w.numberH(h, value) }
func (w *RowWriter) IntH(h NumericField, value int64)                { w.numberH(h, float64(value)) }

func (w *RowWriter) keywordH(h KeywordField, value string, normalized bool) {
	if w.err != nil || h.sf == nil || value == "" {
		return
	}
	v := value
	if !normalized {
		v = normalizeStringByKind(h.sf.special, value, h.sf.opt.Lowercase)
	}
	if v == "" {
		return
	}
	h.sf.fi.exists.Add(w.did)
	if h.col != nil {
		h.col[w.did] = v
	}
	if h.sf.special == specialIP {
		if ip, ok := parseIPv4(v); ok {
			if w.ix.ip4[h.sf.name] == nil {
				w.ix.ip4[h.sf.name] = make(map[DocID]uint32, w.ix.cfg.InitialCapacity)
			}
			w.ix.ip4[h.sf.name][w.did] = ip
			w.ix.ip4Dirty[h.sf.name] = true
		}
	}
	if h.sf.opt.Unique {
		if old, exists := h.sf.fi.unique[v]; exists && !w.ix.deleted.Has(old) && old != w.did {
			w.err = errors.New("unique constraint violation")
			return
		}
		h.sf.fi.unique[v] = w.did
	} else {
		addTerm(h.sf.fi, v, w.did, h.sf.opt.Fuzzy)
	}
	w.ix.indexDerivedLocked(h.sf.fi, h.sf.opt, v, w.did)
}
func (w *RowWriter) textH(h KeywordField, value string, normalized bool) {
	if w.err != nil || h.sf == nil || value == "" {
		return
	}
	v := value
	if !normalized {
		v = normalizeStringByKind(h.sf.special, value, h.sf.opt.Lowercase)
	}
	if v == "" {
		return
	}
	h.sf.fi.exists.Add(w.did)
	if h.col != nil {
		h.col[w.did] = v
	}
	w.ix.tokens = analyzeWith(h.sf.opt, v, w.ix.tokens)
	for pos, t := range w.ix.tokens {
		addTerm(h.sf.fi, t, w.did, h.sf.opt.Fuzzy)
		if h.sf.opt.Phrase {
			addPos(h.sf.fi, t, w.did, uint32(pos))
		}
		w.ix.indexDerivedLocked(h.sf.fi, h.sf.opt, t, w.did)
	}
	if h.sf.opt.Phrase {
		addPhrases(h.sf.fi, w.ix.tokens, w.did)
	}
}
func (w *RowWriter) numberH(h NumericField, value float64) {
	if w.err != nil || h.sf == nil {
		return
	}
	h.sf.fi.exists.Add(w.did)
	if int(w.did) < len(h.vals) {
		h.vals[w.did] = value
		if h.exists != nil {
			h.exists.Add(w.did)
		}
		w.ix.numDirty[h.name] = true
	} else {
		w.ix.setNumericLocked(h.name, w.did, value)
	}
	if h.sf.opt.TTLField {
		w.ix.expires[w.did] = int64(value)
	}
}

func (ix *Index) UpsertKeywordNumericFast(id string, skuH, tenantH KeywordField, priceH NumericField, sku, tenant string, price float64) error {
	w := ix.BeginFast(id)
	w.KeywordHNormalized(skuH, sku)
	w.KeywordHNormalized(tenantH, tenant)
	w.FloatH(priceH, price)
	return w.Commit()
}

type VectorField struct {
	name string
	sf   *schemaField
}

func (ix *Index) VectorField(name string) VectorField {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if id, ok := ix.fieldIDs[name]; ok {
		return VectorField{name: name, sf: &ix.fieldList[id]}
	}
	return VectorField{}
}

func (w *RowWriter) VectorH(h VectorField, value []float64) {
	if w.err != nil || h.sf == nil || len(value) == 0 {
		return
	}
	h.sf.fi.exists.Add(w.did)
	w.ix.addVectorLocked(h.name, w.did, value)
}

func (ix *Index) addVectorLocked(field string, did DocID, vec []float64) {
	if ix.vectors[field] == nil {
		ix.vectors[field] = make(map[DocID][]float64, ix.cfg.InitialCapacity)
	}
	ix.vectors[field][did] = vec
	ann := ix.anns[field]
	if ann == nil {
		dim := len(vec)
		if id, ok := ix.fieldIDs[field]; ok {
			if d := ix.fieldList[id].opt.Dim; d > 0 {
				dim = d
			}
		}
		ann = newVectorANN(dim, "dot", ix.cfg.InitialCapacity)
		ix.anns[field] = ann
	}
	ann.Add(did, vec)
}

// BatchUpsertKeywordNumericFast indexes a common lookup row shape in one tight loop.
// It avoids RowWriter construction and method dispatch per field while keeping the
// same correctness checks as the typed writer path.
func (ix *Index) BatchUpsertKeywordNumericFast(ids, skus []string, skuH, tenantH KeywordField, priceH NumericField, tenant string, prices []float64) error {
	if len(ids) != len(skus) || (prices != nil && len(prices) != len(ids)) {
		return errors.New("ids/skus/prices length mismatch")
	}
	if skuH.sf == nil || tenantH.sf == nil || priceH.sf == nil {
		return errors.New("invalid field handle")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	// For the common low-cardinality tenant batch, promote to / reuse one bitmap
	// once, then use AddUnsafe/Add instead of repeating map lookups per row.
	var tenantBM *Bitmap
	if tenant != "" {
		tfi := tenantH.sf.fi
		if tfi.terms[tenant] != nil {
			tenantBM = tfi.terms[tenant]
		} else {
			tenantBM = NewBitmapCap(tfi.capHint)
			if prev := tfi.termOne[tenant]; prev != 0 {
				tenantBM.Add(prev)
				delete(tfi.termOne, tenant)
			} else {
				tfi.fuzzyTerms[tenant[0]] = append(tfi.fuzzyTerms[tenant[0]], tenant)
			}
			tfi.terms[tenant] = tenantBM
		}
	}
	priceVals := priceH.vals
	priceExists := priceH.exists
	priceDenseDirect := len(priceVals) > 0
	for i, id := range ids {
		if id == "" {
			return errors.New("id required")
		}
		if old, ok := ix.extToDoc[id]; ok {
			ix.deleted.Add(old)
			ix.live.Remove(old)
			ix.hasDeletes = true
		}
		did := ix.nextDocID
		ix.nextDocID++
		ix.extToDoc[id] = did
		ix.docToExt = append(ix.docToExt, id)
		ix.docs = append(ix.docs, nil)
		if int(did>>6) < len(ix.live.words) {
			ix.live.AddUnsafe(did)
		} else {
			ix.live.Add(did)
		}
		// unique sku
		sku := skus[i]
		if sku != "" {
			fi := skuH.sf.fi
			if int(did>>6) < len(fi.exists.words) {
				fi.exists.AddUnsafe(did)
			} else {
				fi.exists.Add(did)
			}
			if old, exists := fi.unique[sku]; exists && !ix.deleted.Has(old) && old != did {
				return errors.New("unique constraint violation")
			}
			fi.unique[sku] = did
			if skuH.sf.opt.Prefix {
				indexPrefixesASCII(fi, sku, did, skuH.sf.fi.capHint)
			}
		}
		if tenantBM != nil {
			fi := tenantH.sf.fi
			if int(did>>6) < len(fi.exists.words) {
				fi.exists.AddUnsafe(did)
			} else {
				fi.exists.Add(did)
			}
			if int(did>>6) < len(tenantBM.words) {
				tenantBM.AddUnsafe(did)
			} else {
				tenantBM.Add(did)
			}
		}
		if int(did>>6) < len(priceH.sf.fi.exists.words) {
			priceH.sf.fi.exists.AddUnsafe(did)
		} else {
			priceH.sf.fi.exists.Add(did)
		}
		price := float64(i)
		if prices != nil {
			price = prices[i]
		}
		if priceDenseDirect && int(did) < len(priceVals) {
			priceVals[did] = price
			if priceExists != nil {
				if int(did>>6) < len(priceExists.words) {
					priceExists.AddUnsafe(did)
				} else {
					priceExists.Add(did)
				}
			}
		} else {
			ix.setNumericLocked(priceH.name, did, price)
		}
	}
	ix.numDirty[priceH.name] = true
	return nil
}

func indexPrefixesASCII(fi *fieldIndex, s string, did DocID, capHint DocID) {
	// Prefix indexing is byte-based for ASCII-normalized lookup keys such as sku,
	// email, domain and IDs. Non-ASCII callers should use the generic analyzer path.
	for i := 1; i <= len(s); i++ {
		addPosting(fi.prefix, fi.prefixOne, s[:i], did, capHint)
	}
}

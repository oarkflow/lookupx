package bench;

import com.fasterxml.jackson.databind.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

public class LuceneBench {
  static final ObjectMapper M = new ObjectMapper();
  static class Row { public String id; public Map<String,String> fields; }
  static class QuerySpec { public String type; public String name; public Map<String,String> fields; public String field; public String value; public String gte; public String lte; public int offset; public int limit = 10; }
  static class QResult { public String name; public long avg_nanos; public int hits; public int loops; QResult(String n,long a,int h,int l){name=n;avg_nanos=a;hits=h;loops=l;} }
  static class Result { public String engine="lucene"; public int rows; public long index_nanos; public double index_rows_per_sec; public long store_bytes; public int warmup_loops=100; public List<QResult> queries = new ArrayList<>(); }

  public static void main(String[] args) throws Exception {
    Map<String,String> a = parseArgs(args);
    String data = a.getOrDefault("--data", "benchdata/dataset.jsonl");
    String queries = a.getOrDefault("--queries", "benchdata/queries.jsonl");
    String fieldsCsv = a.getOrDefault("--fields", "term,group_id,date_key");
    int loops = Integer.parseInt(a.getOrDefault("--loops", "1000"));
    List<String> fields = Arrays.asList(fieldsCsv.split(","));

    Directory dir = new ByteBuffersDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig cfg = new IndexWriterConfig(analyzer);
    cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    long start = System.nanoTime();
    int rows = 0;
    try (IndexWriter w = new IndexWriter(dir, cfg); BufferedReader br = Files.newBufferedReader(Path.of(data))) {
      String line;
      while ((line = br.readLine()) != null) {
        Row r = M.readValue(line, Row.class);
        Document d = new Document();
        d.add(new StringField("id", r.id, Field.Store.NO));
        StringBuilder comp = new StringBuilder();
        for (int i=0;i<fields.size();i++) {
          String f = fields.get(i).trim();
          String v = r.fields.getOrDefault(f, "").toLowerCase(Locale.ROOT);
        if ("client_proc_desc".equals(f)) d.add(new TextField(f, v, Field.Store.NO));
        else if ("work_item_id".equals(f)) d.add(new LongPoint(f, Long.parseLong(v)));
        else if ("event_dos".equals(f)) {
          d.add(new StringField(f, v, Field.Store.NO));
          d.add(new LongPoint("event_dos_epoch", LocalDate.parse(v).atStartOfDay().toEpochSecond(ZoneOffset.UTC)));
        } else d.add(new StringField(f, v, Field.Store.NO));
          if (i>0) comp.append('\u001f');
          comp.append(v);
        }
        d.add(new StringField("__composite", comp.toString(), Field.Store.NO));
        w.addDocument(d);
        rows++;
      }
      w.commit();
    }
    long idx = System.nanoTime() - start;
    long storeBytes = 0;
    for (String name : dir.listAll()) storeBytes += dir.fileLength(name);
    Result out = new Result(); out.rows=rows; out.index_nanos=idx; out.index_rows_per_sec = rows / (idx / 1_000_000_000.0); out.store_bytes=storeBytes;

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      List<QuerySpec> qs = readQueries(queries);
      for (QuerySpec q : qs) {
        org.apache.lucene.search.Query lq;
        String field = q.field == null ? "" : q.field;
        String value = q.value == null ? "" : q.value.toLowerCase(Locale.ROOT);
        if (field.isEmpty() && q.fields != null && !q.fields.isEmpty()) {
          field = q.fields.keySet().iterator().next();
          value = q.fields.get(field).toLowerCase(Locale.ROOT);
        }
        switch (q.type) {
        case "term": lq = new TermQuery(new Term(field, value)); break;
        case "prefix": lq = new PrefixQuery(new Term(field, value)); break;
        case "contains": lq = new WildcardQuery(new Term(field, "*" + value + "*")); break;
        case "phrase": lq = phraseQuery(field, q.value, analyzer, true); break;
        case "text": lq = phraseQuery(field, q.value, analyzer, false); break;
        case "range":
          String rangeField = "event_dos".equals(field) ? "event_dos_epoch" : field;
          long lo = "event_dos".equals(field) ? LocalDate.parse(q.gte).atStartOfDay().toEpochSecond(ZoneOffset.UTC) : Long.parseLong(q.gte);
          long hi = "event_dos".equals(field) ? LocalDate.parse(q.lte).atStartOfDay().toEpochSecond(ZoneOffset.UTC) : Long.parseLong(q.lte);
          lq = LongPoint.newRangeQuery(rangeField, lo, hi); break;
        case "page": lq = new TermQuery(new Term(field, value)); break;
        default:
          StringBuilder comp = new StringBuilder();
          for (int i=0;i<fields.size();i++) {
            String v = q.fields.getOrDefault(fields.get(i).trim(), "").toLowerCase(Locale.ROOT);
            if (i>0) comp.append('\u001f');
            comp.append(v);
          }
          lq = new TermQuery(new Term("__composite", comp.toString()));
          break;
        }
        int limit = q.limit <= 0 ? 10 : q.limit;
        int hits = 0;
        int offset = "page".equals(q.type) ? q.offset : 0;
        for (int i=0;i<out.warmup_loops;i++) searcher.search(lq, limit + offset);
        long qstart = System.nanoTime();
        for (int i=0;i<loops;i++) {
          TopDocs td = searcher.search(lq, limit + offset);
          hits = Math.max(0, td.scoreDocs.length - offset);
        }
        out.queries.add(new QResult(q.name, (System.nanoTime()-qstart)/loops, hits, loops));
      }
    }
    System.out.println(M.writerWithDefaultPrettyPrinter().writeValueAsString(out));
  }
  static org.apache.lucene.search.Query phraseQuery(String field, String value, Analyzer analyzer, boolean phrase) throws Exception {
    List<String> tokens = new ArrayList<>();
    try (TokenStream stream = analyzer.tokenStream(field, value)) {
      CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
      stream.reset(); while (stream.incrementToken()) tokens.add(term.toString()); stream.end();
    }
    if (phrase) {
      PhraseQuery.Builder builder = new PhraseQuery.Builder();
      for (String token : tokens) builder.add(new Term(field, token));
      return builder.build();
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String token : tokens) builder.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD);
    return builder.build();
  }
  static Map<String,String> parseArgs(String[] args){ Map<String,String> m = new HashMap<>(); for(int i=0;i<args.length;i++){ if(args[i].startsWith("--")){ m.put(args[i], i+1<args.length ? args[++i] : ""); } } return m; }
  static List<QuerySpec> readQueries(String p) throws Exception { List<QuerySpec> out = new ArrayList<>(); try(BufferedReader br=Files.newBufferedReader(Path.of(p))){ String line; while((line=br.readLine())!=null) out.add(M.readValue(line, QuerySpec.class)); } return out; }
}

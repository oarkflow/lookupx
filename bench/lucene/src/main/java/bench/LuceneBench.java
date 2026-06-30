package bench;

import com.fasterxml.jackson.databind.*;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class LuceneBench {
  static final ObjectMapper M = new ObjectMapper();
  static class Row { public String id; public Map<String,String> fields; }
  static class QuerySpec { public String name; public Map<String,String> fields; public int limit = 10; }
  static class QResult { public String name; public long avg_nanos; public int hits; public int loops; QResult(String n,long a,int h,int l){name=n;avg_nanos=a;hits=h;loops=l;} }
  static class Result { public String engine="lucene"; public int rows; public long index_nanos; public double index_rows_per_sec; public List<QResult> queries = new ArrayList<>(); }

  public static void main(String[] args) throws Exception {
    Map<String,String> a = parseArgs(args);
    String data = a.getOrDefault("--data", "benchdata/dataset.jsonl");
    String queries = a.getOrDefault("--queries", "benchdata/queries.jsonl");
    String fieldsCsv = a.getOrDefault("--fields", "term,group_id,date_key");
    int loops = Integer.parseInt(a.getOrDefault("--loops", "1000"));
    List<String> fields = Arrays.asList(fieldsCsv.split(","));

    Directory dir = new ByteBuffersDirectory();
    IndexWriterConfig cfg = new IndexWriterConfig(new KeywordAnalyzer());
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
          d.add(new StringField(f, v, Field.Store.NO));
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
    Result out = new Result(); out.rows=rows; out.index_nanos=idx; out.index_rows_per_sec = rows / (idx / 1_000_000_000.0);

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      List<QuerySpec> qs = readQueries(queries);
      for (QuerySpec q : qs) {
        StringBuilder comp = new StringBuilder();
        for (int i=0;i<fields.size();i++) {
          String v = q.fields.getOrDefault(fields.get(i).trim(), "").toLowerCase(Locale.ROOT);
          if (i>0) comp.append('\u001f');
          comp.append(v);
        }
        org.apache.lucene.search.Query lq = new TermQuery(new Term("__composite", comp.toString()));
        int limit = q.limit <= 0 ? 10 : q.limit;
        int hits = 0;
        long qstart = System.nanoTime();
        for (int i=0;i<loops;i++) {
          TopDocs td = searcher.search(lq, limit);
          hits = Math.toIntExact(td.totalHits.value());
        }
        out.queries.add(new QResult(q.name, (System.nanoTime()-qstart)/loops, hits, loops));
      }
    }
    System.out.println(M.writerWithDefaultPrettyPrinter().writeValueAsString(out));
  }
  static Map<String,String> parseArgs(String[] args){ Map<String,String> m = new HashMap<>(); for(int i=0;i<args.length;i++){ if(args[i].startsWith("--")){ m.put(args[i], i+1<args.length ? args[++i] : ""); } } return m; }
  static List<QuerySpec> readQueries(String p) throws Exception { List<QuerySpec> out = new ArrayList<>(); try(BufferedReader br=Files.newBufferedReader(Path.of(p))){ String line; while((line=br.readLine())!=null) out.add(M.readValue(line, QuerySpec.class)); } return out; }
}

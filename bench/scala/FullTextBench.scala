package clear.dao

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, StoredField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.ByteBuffersDirectory

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Benchmark adapter for Clear's production FullTextDao.searchCpt on a shared JSONL fixture.
  * Index documents use the field layout from FullTextDao.indexCpt; database cursor/SQL time
  * is deliberately not included because LookupX consumes the same exported rows directly.
  */
object FullTextBench {
  private val mapper = new ObjectMapper()

  case class Query(name: String, field: String, value: String, limit: Int, workItem: Option[Int])
  case class Result(engine: String, rows: Int, index_nanos: Long, index_rows_per_sec: Double,
                    warmup_loops: Int, queries: Seq[QResult])
  case class QResult(name: String, avg_nanos: Long, hits: Int, top_ids: Seq[String], loops: Int)

  def main(args: Array[String]): Unit = {
    val opts = args.sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap
    val dataPath = Paths.get(opts.getOrElse("--data", "benchdata/dataset.jsonl"))
    val queryPath = Paths.get(opts.getOrElse("--queries", "benchdata/queries.jsonl"))
    val loops = opts.getOrElse("--loops", "2000").toInt
    val rows = Files.readAllLines(dataPath).asScala.filter(_.nonEmpty).map(mapper.readTree(_)).toVector
    val queries = Files.readAllLines(queryPath).asScala.filter(_.nonEmpty).map(parseQuery).toVector
    val dao = FullTextDao(null, buildIndexes = false)
    val directory = new ByteBuffersDirectory()
    val writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))

    val indexStart = System.nanoTime()
    rows.foreach { row =>
      val fields = row.path("fields")
      val code = fields.path("cpt_code").asText().toUpperCase
      val codeLc = code.toLowerCase
      val desc = fields.path("client_proc_desc").asText()
      val doc = new Document()
      doc.add(new StringField(dao.CODE_FIELD, code, Field.Store.YES))
      if (codeLc != code) doc.add(new StringField(dao.CODE_LOWERCASE_FIELD, codeLc, Field.Store.NO))
      if (code.contains(".")) doc.add(new StringField(dao.CODE_NO_PERIOD_FIELD, codeLc.filterNot(_ == '.'), Field.Store.NO))
      doc.add(new StringField(dao.DESC_FIELD, s"$code – $desc", Field.Store.YES))
      doc.add(new TextField(dao.FULLTEXT_FIELD, desc, Field.Store.NO))
      doc.add(new StringField(dao.WORKITEM_FIELD, fields.path("work_item_id").asText(), Field.Store.YES))
      doc.add(new StringField(dao.EFFECTIVE_DATE_FIELD, fields.path("event_dos").asText(), Field.Store.YES))
      doc.add(new StringField(dao.END_EFFECTIVE_DATE_FIELD, "NULL", Field.Store.YES))
      doc.add(new StringField(dao.PATIENT_STATUS_UID, fields.path("patient_status").asText(), Field.Store.YES))
      doc.add(new StringField(dao.CHARGE_TYPE, "CPT", Field.Store.YES))
      writer.addDocument(doc)
    }
    writer.close()
    val indexNanos = System.nanoTime() - indexStart
    val searcher = new IndexSearcher(DirectoryReader.open(directory))

    val measured = queries.map { q =>
      val resultFn = () => runQuery(dao, searcher, q)
      (0 until 100).foreach(_ => resultFn())
      val start = System.nanoTime()
      var found = List.empty[String]
      (0 until loops).foreach(_ => found = resultFn())
      QResult(q.name, (System.nanoTime() - start) / loops, found.size, found, loops)
    }
    val out = Result("clear-scala-fulltext", rows.size, indexNanos,
      rows.size / (indexNanos / 1e9), 100, measured)
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(out))
    searcher.getIndexReader.close()
    directory.close()
  }

  private def parseQuery(line: String): Query = {
    val n = mapper.readTree(line)
    val wi = if (n.has("work_item_id")) Some(n.path("work_item_id").asInt()) else None
    Query(n.path("name").asText(), n.path("field").asText(), n.path("value").asText(),
      if (n.path("limit").asInt() > 0) n.path("limit").asInt() else 100, wi)
  }

  private def runQuery(dao: FullTextDao, searcher: IndexSearcher, q: Query): List[String] = {
    // Mirrors the actual code path: exact, code-prefix, and analyzed description-prefix
    // lookups, with each stage capped at maxResults and then deduplicated by LookupItemCpt.
    val found = dao.searchCpt(searcher, q.workItem, List(q.value), q.limit)
    found.map(item => s"${item.value}:${item.work_item_uid}")
  }
}

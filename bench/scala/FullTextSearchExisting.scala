import clear.dao.FullTextDao
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.index.DirectoryReader

import java.nio.file.Paths
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters._

object FullTextSearchExisting {
  def main(args: Array[String]): Unit = {
    val path = Paths.get(args(0))
    val workItem = if (args(1).equalsIgnoreCase("none")) None else Some(args(1).toInt)
    val keyword = args(2)
    val loops = args(3).toInt
    val workers = if (args.length > 4) args(4).toInt else 1
    val dao = FullTextDao(null, buildIndexes = false)
    val directory = new MMapDirectory(path)
    val searcher = new IndexSearcher(DirectoryReader.open(directory))
    val runSearch = () => dao.searchCpt(searcher, workItem, List(keyword), 100)
      .map(item => s"${item.work_item_uid}:${Option(item.charge_type).flatten.getOrElse("")}:${item.effective_date.take(10)}:${item.value}")
    (0 until 100).foreach(_ => runSearch())
    require(workers > 0 && loops > 0, "loops and workers must be positive")
    val pool = Executors.newFixedThreadPool(workers)
    val ready = new CountDownLatch(workers)
    val startGate = new CountDownLatch(1)
    val samples = new ConcurrentLinkedQueue[java.lang.Long]()
    val firstIDs = new AtomicReference[List[String]](Nil)
    val tasks: Seq[java.util.concurrent.Future[_]] = (0 until workers).map { _ =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          ready.countDown()
          startGate.await()
          (0 until loops).foreach { _ =>
            val started = System.nanoTime()
            val rows = runSearch()
            samples.add(java.lang.Long.valueOf(System.nanoTime() - started))
            firstIDs.compareAndSet(Nil, rows)
          }
        }
      })
    }
    ready.await()
    val start = System.nanoTime()
    startGate.countDown()
    tasks.foreach(_.get())
    val elapsed = System.nanoTime() - start
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.MINUTES)
    val sorted = samples.asScala.map(_.longValue()).toVector.sorted
    val total = sorted.size
    def percentile(p: Double): Long = sorted(math.min(total - 1, math.floor((total - 1) * p).toInt))
    val avg = sorted.sum / total
    val rows = firstIDs.get()
    println(s"engine=clear-scala-fulltext workers=$workers requests=$total hits=${rows.size} avg_nanos=$avg p50_nanos=${percentile(.50)} p95_nanos=${percentile(.95)} qps=${total * 1e9 / elapsed} elapsed_nanos=$elapsed top_ids=${rows.take(10).mkString(",")}")
    searcher.getIndexReader.close()
    directory.close()
    System.exit(0)
  }
}

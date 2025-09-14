package com.example.pipeline.app

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/** End-to-end test for the Spark pipeline.
 * Generates deterministic CSV inputs with 10k transactions, runs the job,
 * and validates that both outputs exist and aggregates are correct.
 */
final class EndToEndSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("EndToEndSpec")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("pipeline produces both outputs with correct aggregates (10k rows)") {
    // temp folders
    val base   = Files.createTempDirectory("pipeline-e2e").toFile.getAbsolutePath
    val inDir  = Paths.get(base, "in").toFile
    val outDir = Paths.get(base, "out").toFile
    inDir.mkdirs(); outDir.mkdirs()

    // products.csv (3 categories)
    val productsCSV =
      """productId,category
        |p1,Electronics
        |p2,Books
        |p3,Toys
        |""".stripMargin
    Files.write(
      Paths.get(inDir.getAbsolutePath, "products.csv"),
      productsCSV.getBytes(StandardCharsets.UTF_8)
    )

    // transactions.csv with 10,000 rows (round-robin over p1/p2/p3)
    val transactionsCSV = new StringBuilder("txnId,userId,productId,quantity,unitPrice,timestamp\n")
    var i = 1
    while (i <= 10000) {
      val (pid, price) =
        if (i % 3 == 1) ("p1", 100.0)
        else if (i % 3 == 2) ("p2", 50.0)
        else ("p3", 20.0)
      transactionsCSV.append(s"t$i,u${(i % 5000) + 1},$pid,1,$price,1700000000\n")
      i += 1
    }
    Files.write(
      Paths.get(inDir.getAbsolutePath, "transactions.csv"),
      transactionsCSV.toString.getBytes(StandardCharsets.UTF_8)
    )

    // expected revenue per category
    val expected = Map(
      "Electronics" -> (3334 * 100.0), // 10000 % 3 == 1
      "Books"       -> (3333 * 50.0),
      "Toys"        -> (3333 * 20.0)
    )

    // run pipeline
    SparkJobs.runPipeline(
      spark,
      txnsPath           = Paths.get(inDir.getAbsolutePath, "transactions.csv").toString,
      productsPath       = Paths.get(inDir.getAbsolutePath, "products.csv").toString,
      outDir             = outDir.getAbsolutePath,
      minTotalThreshold  = 0.0
    )

    // validate outputs exist
    val revenueByCategoryDir  = Paths.get(outDir.getAbsolutePath, "revenue_by_category").toString
    val revenuePureDir        = Paths.get(outDir.getAbsolutePath, "revenue_pure").toString
    assert(Files.exists(Paths.get(revenueByCategoryDir)),  "revenue_by_category output folder not found")
    assert(Files.exists(Paths.get(revenuePureDir)),        "revenue_pure output folder not found")

    // read results
    import org.apache.spark.sql.functions._
    val revenueByCategoryDF = spark.read.option("header", "true").csv(revenueByCategoryDir)
    val gotMap = revenueByCategoryDF
      .select(col("category"), col("revenue").cast("double"))
      .collect()
      .map(r => r.getString(0) -> r.getDouble(1))
      .toMap

    assert(gotMap.keySet == expected.keySet, s"categories mismatch. got=${gotMap.keySet} expected=${expected.keySet}")
    expected.foreach { case (cat, exp) =>
      val delta = math.abs(gotMap(cat) - exp)
      assert(delta < 1e-6, s"wrong revenue for $cat: got=${gotMap(cat)} expected=$exp")
    }

    // cross-check the pure output categories
    val revenuePureDF = spark.read.option("header", "true").csv(revenuePureDir)
    val pureCats = revenuePureDF.select(col("category")).collect().map(_.getString(0)).toSet
    assert(pureCats == expected.keySet, s"pure output categories mismatch. got=$pureCats")
  }
}







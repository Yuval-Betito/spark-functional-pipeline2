package com.example.pipeline.tools

import java.nio.file.{Files, Paths, Path}
import scala.io.Source
import scala.collection.JavaConverters._

/** Verifier tool for the Spark pipeline outputs.
 *
 * Compares the numeric values in:
 *  - `revenue_by_category/part-*` (last column = revenue)
 *  - `revenue_pure/part-*`       (last column = revenue_pure)
 *
 * Prints `VERIFY OK` when all categories match within a small epsilon,
 * otherwise prints a detailed diff per category and exits with code 1.
 */
object Verify {

  /** Relative numeric tolerance for comparisons. */
  private val epsilon: Double = 1e-9

  /** Entry point: verifies `revenue_by_category` vs `revenue_pure` under `baseDir`.
   *
   * @param args args(0) optional — base output directory (default: `"data/out/summary"`).
   */
  def main(args: Array[String]): Unit = {
    require(args != null, "args must not be null")
    require(epsilon >= 0.0, "epsilon must be non-negative")

    // ---------- Args & paths ----------
    val baseDir: String = if (args.nonEmpty) args(0) else "out"
    require(baseDir.nonEmpty, "baseDir must not be empty")

    val dirBy: String   = s"$baseDir/revenue_by_category"
    val dirPure: String = s"$baseDir/revenue_pure"

    // ---------- Locate part files ----------
    val revenueByPath: Path   = firstPart(dirBy)   // category,num_txn,revenue -> last column (revenue)
    val revenuePurePath: Path = firstPart(dirPure) // category,revenue_pure     -> last column (revenue_pure)

    // ---------- Load CSVs into maps (category -> value) ----------
    val revenueByMap: Map[String, Double]   = readLastNumericColumnToMap(revenueByPath)
    val revenuePureMap: Map[String, Double] = readLastNumericColumnToMap(revenuePurePath)

    // ---------- Compare with tolerance ----------
    val keys: Set[String] = revenueByMap.keySet ++ revenuePureMap.keySet
    val diffs: Seq[String] = keys.toSeq.sorted.flatMap { k =>
      val byVal   = revenueByMap.getOrElse(k, Double.NaN)
      val pureVal = revenuePureMap.getOrElse(k, Double.NaN)
      val delta   = math.abs(byVal - pureVal)
      if (delta > epsilon || byVal.isNaN || pureVal.isNaN)
        Some(f"$k%-12s  by=$byVal%.12f  pure=$pureVal%.12f  \u0394=$delta%.12g")
      else None
    }

    if (diffs.isEmpty) {
      println("VERIFY OK")
      sys.exit(0)
    } else {
      println("VERIFY FAILED:")
      diffs.foreach(println)
      sys.exit(1)
    }
  }

  /** Returns the first `part-*` file under the given directory.
   *
   * @param dir directory that contains Spark CSV output files
   * @throws RuntimeException if no `part-*` file exists in `dir`
   */
  private def firstPart(dir: String): Path = {
    require(dir != null && dir.nonEmpty, "dir must not be null or empty")
    val p: Path = Paths.get(dir)
    require(Files.exists(p) && Files.isDirectory(p), s"Directory does not exist: $dir")

    // Close underlying stream to avoid resource leak
    val stream = Files.list(p)
    try {
      val it = stream.iterator().asScala
      it.find(_.getFileName.toString.startsWith("part-"))
        .getOrElse(sys.error(s"No part-* file found in $dir"))
    } finally {
      stream.close()
    }
  }

  /** Reads a CSV and returns a map from the first column (key)
   * to the **last** column parsed as `Double`.
   *
   * Notes:
   *  - First line is treated as a header and skipped.
   *  - Splits by comma with `split(",", -1)` to allow empty columns.
   *
   * @param file path to a CSV `part-*` file
   * @return a map of key → last numeric value
   * @throws RuntimeException on empty file or malformed number
   */
  private def readLastNumericColumnToMap(file: Path): Map[String, Double] = {
    require(file != null, "file must not be null")
    require(Files.exists(file) && Files.isRegularFile(file), s"File not found: $file")

    val src = Source.fromFile(file.toFile)("UTF-8")
    try {
      val lines = src.getLines()
      if (!lines.hasNext) sys.error(s"Empty file: $file")
      val _ = lines.next() // drop header
      lines.map { ln =>
        val cols = ln.split(",", -1)
        val key  = cols(0)
        val v    = cols.last.toDouble
        key -> v
      }.toMap
    } finally {
      src.close()
    }
  }
}

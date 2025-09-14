package com.example.pipeline.app

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.util.Random
import scala.util.Try

/** Synthetic data generator.
 *
 * Creates a `data/` directory and writes:
 *  - `products.csv`  (productId, category)
 *  - `transactions.csv` (txnId, userId, productId, quantity, unitPrice, timestamp)
 *
 * Default number of transactions: 20,000 (overridable via CLI arg 0).
 */
object DataGen {

  /** Format a double with two fraction digits.
   * @param d value
   * @return  string formatted to two decimals
   */
  private def fmt(d: Double): String = f"$d%.2f"

  /** Program entry point.
   * @param args args(0) optionally sets the number of transactions to generate
   */
  def main(args: Array[String]): Unit = {
    // Robust CLI parsing: default 20000 if missing/invalid
    val numTransactions: Int =
      if (args.nonEmpty) Try(args(0).toInt).toOption.getOrElse(20000) else 20000

    /* -----------------------------
     * Ensure data directory exists
     * --------------------------- */
    val dataDir = Paths.get("data")
    if (!Files.exists(dataDir)) Files.createDirectories(dataDir)

    /* -----------------------------
     * products.csv
     * --------------------------- */
    val products = Vector(
      "p1"  -> "Books",
      "p2"  -> "Electronics",
      "p3"  -> "Beauty",
      "p4"  -> "Toys",
      "p5"  -> "Home",
      "p6"  -> "Sports",
      "p7"  -> "Grocery",
      "p8"  -> "Clothing",
      "p9"  -> "Garden",
      "p10" -> "Automotive"
    )

    val productsCSV =
      ("productId,category" +: products.map { case (id, cat) => s"$id,$cat" })
        .mkString("\n")

    Files.write(
      dataDir.resolve("products.csv"),
      productsCSV.getBytes(StandardCharsets.UTF_8)
    )

    /* -----------------------------
     * transactions.csv
     * --------------------------- */
    val random = new Random(42) // reproducible
    val now = Instant.now().toEpochMilli
    val header = "txnId,userId,productId,quantity,unitPrice,timestamp"

    val rows = (1 to numTransactions).iterator.map { i =>
      val (pid, _) = products(random.nextInt(products.length))
      val qty      = 1 + random.nextInt(5)
      val price    = 5 + random.nextInt(300) + random.nextDouble()
      val user     = s"u${1 + random.nextInt(5000)}"
      val ts       = now - random.nextInt(60 * 60 * 24 * 30) * 1000L
      s"t$i,$user,$pid,$qty,${fmt(price)},$ts"
    }

    val txnsCSV = (header +: rows.toSeq).mkString("\n")
    Files.write(
      dataDir.resolve("transactions.csv"),
      txnsCSV.getBytes(StandardCharsets.UTF_8)
    )

    println(s"[OK] Wrote data/products.csv and data/transactions.csv (rows=$numTransactions)")
  }
}







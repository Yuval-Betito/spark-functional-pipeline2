package com.example.pipeline.app

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.util.VersionInfo
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Application entry point for the Spark functional pipeline.
 *
 * Provides a thin CLI wrapper around [[SparkJobs.runPipeline]] and prints
 * basic environment details for easier troubleshooting on Windows.
 */
object Main {

  /**
   * Builds a local SparkSession for this application.
   *
   * @param appName the Spark application name to set on the session
   * @return a configured [[SparkSession]] bound to `local[*]`
   */
  private def buildSpark(appName: String): SparkSession = {
    require(appName.nonEmpty, "appName must not be empty")
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Prints environment information useful for diagnosing Windows setups.
   *
   * Side effects: writes a short report to STDOUT.
   */
  private def printEnvironmentInfo(): Unit = {
    val sparkVer   = org.apache.spark.SPARK_VERSION
    val hadoopVer  = VersionInfo.getVersion
    val hadoopHome = sys.env.getOrElse("HADOOP_HOME", "N/A")
    val hadoopProp = System.getProperty("hadoop.home.dir", "N/A")
    val javaLib    = System.getProperty("java.library.path", "")

    val winutils   = Paths.get(hadoopHome, "bin", "winutils.exe")
    val hadoopDll  = Paths.get(hadoopHome, "bin", "hadoop.dll")

    println("-----------------------------------------------------------")
    println(s"Spark version          : $sparkVer")
    println(s"Hadoop client version  : $hadoopVer")
    println()
    println(s"HADOOP_HOME (env)      : $hadoopHome")
    println(s"hadoop.home.dir (prop) : $hadoopProp")
    println(s"java.library.path      : $javaLib")
    if (hadoopHome != "N/A") {
      println(s"Check $winutils : " + (if (Files.exists(winutils)) "OK" else "MISSING"))
      println(s"Check $hadoopDll: " + (if (Files.exists(hadoopDll)) "OK" else "MISSING"))
    }
    println("-----------------------------------------------------------")
  }

  /**
   * Program entry point.
   *
   * CLI arguments (all optional):
   *   - args(0): path to `transactions.csv` (default: `data/transactions.csv`)
   *   - args(1): path to `products.csv`     (default: `data/products.csv`)
   *   - args(2): output directory           (default: `out`)
   *   - args(3): min total threshold        (default: `0.0`)
   *
   * @param args command line arguments as described above
   */
  def main(args: Array[String]): Unit = {
    // Parse CLI args (robust defaults)
    val transactionsCSVPath = args.headOption.getOrElse("data/transactions.csv")
    val productsCSVPath     = args.lift(1).getOrElse("data/products.csv")
    val outputDir           = args.lift(2).getOrElse("out")
    val minTotalThreshold   = args.lift(3).flatMap(s => Try(s.toDouble).toOption).getOrElse(0.0)

    // Print environment info (helpful on Windows)
    printEnvironmentInfo()

    // Bootstrap SparkSession
    val spark = buildSpark("spark-functional-pipeline")

    spark.sparkContext.setLogLevel("WARN")
    System.setProperty("org.apache.spark.ui.showConsoleProgress", "false")

    try {
      // Run the pipeline (single place to orchestrate the job)
      SparkJobs.runPipeline(
        spark             = spark,
        txnsPath          = transactionsCSVPath,
        productsPath      = productsCSVPath,
        outDir            = outputDir,
        minTotalThreshold = minTotalThreshold
      )
      println(s"Pipeline finished successfully. Outputs written to '$outputDir'.")
    } finally {
      // Cleanup
      spark.stop()
    }
  }
}





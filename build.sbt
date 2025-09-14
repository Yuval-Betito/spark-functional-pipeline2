// === Project coordinates & Scala pin ===
ThisBuild / organization := "com.example"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.19"

// === Java 11 (per syllabus) ===
ThisBuild / javacOptions ++= Seq("--release", "11")
ThisBuild / scalacOptions ++= Seq(
  "-deprecation", "-feature", "-unchecked", "-Xlint",
  "-Ywarn-dead-code", "-Ywarn-numeric-widen"
)

// === Versions ===
lazy val sparkVersion  = "3.5.1"
lazy val hadoopVersion = "3.3.5"

// === Dependencies ===
// Keep Spark not `provided` for local tests. For cluster builds, switch to % "provided".
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion,
  "org.apache.spark" %% "spark-sql"     % sparkVersion,
  "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
  "org.scalatest"    %% "scalatest"     % "3.2.19" % Test,

  // Lock Hadoop to avoid transitive conflicts in local/Windows environments
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)

ThisBuild / dependencyOverrides ++= Seq(
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)

// === Test runtime stability for Spark ===
Test / parallelExecution := false        // do not run suites in parallel
Test / fork              := true         // each suite in its own JVM (Spark-friendly)
Compile / run / fork     := true         // runMain in a separate JVM

// === JDK opens (silence reflective-access warnings on JDK 11) ===
val jdkOpens = Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
)

// === Memory & GC (recommended) ===
Test / javaOptions ++= Seq("-Xms512m", "-Xmx2g") ++ jdkOpens
Compile / run / javaOptions ++= Seq("-Xms512m", "-Xmx2g") ++ jdkOpens

// === Stable local binding for Spark on Windows ===
Test / envVars ++= Map("SPARK_LOCAL_IP" -> "127.0.0.1")
Compile / run / envVars ++= Map("SPARK_LOCAL_IP" -> "127.0.0.1")

// === Scaladoc noise reduction ===
Compile / doc / scalacOptions ++= Seq("-no-link-warnings")

// === Logging (force log4j2 config & unify output) ===
// Load log4j2.properties from classpath for both run and tests.
Compile / run / javaOptions ++= Seq(
  "-Dlog4j.configurationFile=classpath:log4j2.properties",
  "-Dspark.ui.showConsoleProgress=false"
)
Test / javaOptions ++= Seq(
  "-Dlog4j.configurationFile=classpath:log4j2.properties",
  "-Dspark.ui.showConsoleProgress=false"
)

// Send child process stderr to stdout (cosmetic: avoids `[error]` tag on INFO)
Compile / run / outputStrategy := Some(StdoutOutput)
Test    / outputStrategy := Some(StdoutOutput)


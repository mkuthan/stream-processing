import sbt._

object Dependencies {
  private val scioVersion = "0.11.11"
  private val logbackVersion = "1.4.4"
  private val scalaTestVersion = "3.2.14"
  private val json4sVersion = "4.0.6"

  // Beam is not compatible with google-api-client 2.0.0
  // Pin to the latest cloud API libraries with dependency to google-api-client 1.35.2
  // See: https://github.com/apache/beam/issues/22843
  private val googleCloudBigQueryVersion = "2.14.1" // scala-steward:off

  val scio = "com.spotify" %% "scio-core" % scioVersion
  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % scioVersion
  val scioTest = "com.spotify" %% "scio-test" % scioVersion

  val logback = "ch.qos.logback" % "logback-classic" % logbackVersion

  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  val json4s = "org.json4s" %% "json4s-jackson" % json4sVersion
  val json4sExt = "org.json4s" %% "json4s-ext" % json4sVersion

  val googleCloudBigQuery = "com.google.cloud" % "google-cloud-bigquery" % googleCloudBigQueryVersion
}

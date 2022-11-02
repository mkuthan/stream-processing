import sbt._

object Dependencies {
  private val scioVersion = "0.11.11"
  private val logbackVersion = "1.4.4"
  private val scalaTestVersion = "3.2.14"
  private val json4sVersion = "4.0.6"

  val scio = "com.spotify" %% "scio-core" % scioVersion
  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % scioVersion
  val scioTest = "com.spotify" %% "scio-test" % scioVersion

  val logback = "ch.qos.logback" % "logback-classic" % logbackVersion

  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  val json4s = "org.json4s" %% "json4s-jackson" % json4sVersion
  val json4sExt = "org.json4s" %% "json4s-ext" % json4sVersion
}

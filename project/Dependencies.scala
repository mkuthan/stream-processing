import sbt._

object Dependencies {
  val scio = "com.spotify" %% "scio-core" % "0.12.5"
  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % "0.12.5"
  val scioTest = "com.spotify" %% "scio-test" % "0.12.5"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

  val slf4j = "org.slf4j" % "slf4j-api" % "2.0.6"
  val slf4jJcl = "org.slf4j" % "jcl-over-slf4j" % "2.0.6"

  val logback = "ch.qos.logback" % "logback-classic" % "1.4.5"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % "3.2.15.0"

  val diffx = "com.softwaremill.diffx" %% "diffx-scalatest-should" % "0.7.0"

  val json4s = "org.json4s" %% "json4s-jackson" % "4.0.6"
  val json4sExt = "org.json4s" %% "json4s-ext" % "4.0.6"
}

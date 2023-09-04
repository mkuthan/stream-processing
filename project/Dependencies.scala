import sbt._

object Dependencies {
  val scio = "com.spotify" %% "scio-core" % "0.13.3"
  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % "0.13.3"
  val scioTest = "com.spotify" %% "scio-test" % "0.13.3"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

  val slf4j = "org.slf4j" % "slf4j-api" % "2.0.7"
  val slf4jJcl = "org.slf4j" % "jcl-over-slf4j" % "2.0.7"

  val logback = "ch.qos.logback" % "logback-classic" % "1.4.9"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.16"
  val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0"

  val magnolifyScalaCheck = "com.spotify" %% "magnolify-scalacheck" % "0.6.2"

  val diffx = "com.softwaremill.diffx" %% "diffx-scalatest-should" % "0.8.3"
}

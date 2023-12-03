import sbt._

object Dependencies {
  val scio = "com.spotify" %% "scio-core" % "0.13.6"

  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % "0.13.6" excludeAll (
    ExclusionRule(organization = "org.apache.beam", name = "beam-runners-direct-java")
  )

  val scioTest = "com.spotify" %% "scio-test" % "0.13.6" excludeAll (
    ExclusionRule(organization = "org.apache.beam", name = "beam-runners-direct-java")
  )

  val beamDirectRunner = "org.apache.beam" % "beam-runners-direct-java" % "2.52.0"
  val beamDataflowRunner = "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.52.0"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

  val slf4j = "org.slf4j" % "slf4j-api" % "2.0.9"
  val slf4jJcl = "org.slf4j" % "jcl-over-slf4j" % "2.0.9"

  val logback = "ch.qos.logback" % "logback-classic" % "1.4.14"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17"
  val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % "3.2.17.0"

  val magnolifyScalaCheck = "com.spotify" %% "magnolify-scalacheck" % "0.6.4"

  val diffx = "com.softwaremill.diffx" %% "diffx-scalatest-should" % "0.9.0"
}

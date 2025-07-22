import sbt.*

object Dependencies {
  val scioVersion = "0.14.18"
  val beamVersion = "2.66.0"

  val scio = "com.spotify" %% "scio-core" % scioVersion

  val scioGcp = "com.spotify" %% "scio-google-cloud-platform" % scioVersion excludeAll (
    ExclusionRule(organization = "org.apache.beam", name = "beam-runners-direct-java")
  )

  val scioTest = "com.spotify" %% "scio-test" % scioVersion excludeAll (
    ExclusionRule(organization = "org.apache.beam", name = "beam-runners-direct-java")
  )

  val beamDirectRunner = "org.apache.beam" % "beam-runners-direct-java" % beamVersion
  val beamDataflowRunner = "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

  val slf4j = "org.slf4j" % "slf4j-api" % "2.0.17"
  val slf4jJcl = "org.slf4j" % "jcl-over-slf4j" % "2.0.17"

  val logback = "ch.qos.logback" % "logback-classic" % "1.5.18"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19"
  val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0"

  val magnolifyScalaCheck = "com.spotify" %% "magnolify-scalacheck" % "0.8.0"

  val diffx = "com.softwaremill.diffx" %% "diffx-scalatest-should" % "0.9.0"
}

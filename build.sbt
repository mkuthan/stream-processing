name := "stream-processing"
version := "1.0"

scalaVersion := "2.13.8"

val scioVersion = "0.11.10"

libraryDependencies ++= Seq(
  // scio
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  // other
  "ch.qos.logback" % "logback-classic" % "1.4.0",
  // tests
  "org.scalatest" %% "scalatest" % "3.2.13" % "test"
)

// enable XML report for Jacoco, needed by codecov.io
jacocoReportSettings := JacocoReportSettings(
  "Jacoco Coverage Report",
  None,
  JacocoThresholds(),
  Seq(JacocoReportFormats.XML, JacocoReportFormats.HTML),
  "utf-8"
)

// specify scapegoat version explicitly (sbt plugins assumes 1.0.0)
ThisBuild / scapegoatVersion := "1.4.15"

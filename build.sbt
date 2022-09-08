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

// automatically reload the build when source changes are detected
Global / onChangedBuildSource := ReloadOnSourceChanges

// enable XML report, needed by codecov.io
jacocoReportSettings := JacocoReportSettings()
  .withFormats(JacocoReportFormats.XML, JacocoReportFormats.HTML)

// configure static code analysis
val disabledCompileWarts = Seq()
val disabledTestWarts = Seq(
  Wart.Any,
  Wart.NonUnitStatements,
  Wart.Nothing
)

Compile / compile / wartremoverErrors := Warts.allBut(disabledCompileWarts: _*)
Test / compile / wartremoverErrors := Warts.allBut(disabledTestWarts: _*)

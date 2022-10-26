name := "stream-processing"
version := "1.0"

scalaVersion := "2.13.10"

val scioVersion = "0.11.11"
val json4sVersion = "4.0.6"
val scalaTestVersion = "3.2.14"
val logbackVersion = "1.4.4"

libraryDependencies ++= Seq(
  // scio
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  // other
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.json4s" %% "json4s-ext" % json4sVersion,
  // tests
  "org.scalatest" %% "scalatest" % scalaTestVersion % "it, test"
)

// recommended options for scalac
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Ymacro-annotations", // required by Scio macros
  "-Xmacro-settings:show-coder-fallback=true" // warn about fallback to Kryo coder
)

// automatically reload the build when source changes are detected
Global / onChangedBuildSource := ReloadOnSourceChanges

// enable XML report, needed by codecov.io
jacocoReportSettings := JacocoReportSettings()
  .withFormats(JacocoReportFormats.XML, JacocoReportFormats.HTML)

// configure static code analysis, Scio macros require relaxed rules
val disabledWarts = Seq(
  Wart.Any,
  Wart.DefaultArguments,
  Wart.FinalCaseClass,
  Wart.LeakingSealed,
  Wart.NonUnitStatements,
  Wart.Nothing,
  Wart.Throw,
  Wart.ToString
)

Compile / compile / wartremoverErrors := Warts.allBut(disabledWarts: _*)
Test / compile / wartremoverErrors := Warts.allBut(disabledWarts: _*)

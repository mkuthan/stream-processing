name := "example-streaming"
version := "1.0"

scalaVersion := "2.13.8"

val scioVersion = "0.11.9"

libraryDependencies ++= Seq(
  // scio
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  // other
  "ch.qos.logback" % "logback-classic" % "1.4.0",
  // tests
  "org.scalatest" %% "scalatest" % "3.2.12" % "test"
)

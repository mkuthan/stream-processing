name := "example-streaming"
version := "1.0"

scalaVersion := "2.13.8"

val scioVersion = "0.11.7"

libraryDependencies ++= Seq(
  // scio
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  // other
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  // tests
  "org.scalatest" %% "scalatest" % "3.2.11" % "test"
)

name := "example-streaming"
version := "1.0"

scalaVersion := "2.13.3"

val scioVersion = "0.11.4"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",

  "ch.qos.logback" % "logback-classic" % "1.2.10",

  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
)
// test coverage
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.5.0")
// code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
// code analysis
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.7")
// automated code changes
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")
// fat-jar for flex templates
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

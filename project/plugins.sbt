// test coverage
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.5.0")
// code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
// code analysis
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.13")
// automated code changes
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.3")
// fat-jar for flex templates
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

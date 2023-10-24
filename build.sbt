import sbt._

import Dependencies._
import Settings._

addCommandAlias("check", "clean; scalafixAll; scalafmtAll; scapegoat; testOnly -- -l org.scalatest.tags.Slow")

lazy val root = (project in file("."))
  .settings(
    name := "stream-processing",
    commonSettings
  ).aggregate(test, shared, infrastructure, wordCount, userSessions, tollDomain, tollApplication)

lazy val test = (project in file("stream-processing-test"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      scio,
      scioGcp,
      scioTest,
      scalaLogging,
      slf4j,
      slf4jJcl,
      logback,
      scalaTest,
      scalaTestPlusScalaCheck,
      beamDirectRunner % Test
    )
  )

lazy val shared = (project in file("stream-processing-shared"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      scio,
      scioGcp,
      scalaLogging,
      slf4j,
      slf4jJcl,
      logback,
      beamDirectRunner % Test
    )
  )
  .dependsOn(test % Test)

lazy val infrastructure = (project in file("stream-processing-infrastructure"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      scio,
      scioGcp,
      scalaLogging,
      slf4j,
      slf4jJcl,
      logback,
      beamDirectRunner % Test,
      magnolifyScalaCheck % Test,
      diffx % Test
    )
  )
  .dependsOn(
    shared,
    test % Test
  )

lazy val wordCount = (project in file("word-count"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      beamDirectRunner % Test
    )
  )
  .dependsOn(
    shared,
    test % Test
  )

lazy val userSessions = (project in file("user-sessions"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      beamDirectRunner % Test
    )
  )
  .dependsOn(
    shared,
    test % Test
  )

lazy val tollDomain = (project in file("toll-domain"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      beamDirectRunner % Test
    )
  )
  .dependsOn(
    shared,
    test % Test
  )

lazy val tollApplication = (project in file("toll-application"))
  .settings(
    commonSettings,
    assemblySettings,
    libraryDependencies ++= Seq(
      beamDirectRunner % Test,
      beamDataflowRunner % Runtime
    )
  )
  .dependsOn(
    shared,
    infrastructure,
    test % Test,
    tollDomain % "compile->compile;test->test"
  )

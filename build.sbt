import sbt._

import Dependencies._
import Settings._

lazy val root = (project in file("."))
  .settings(
    name := "stream-processing",
    commonSettings
  ).aggregate(test, shared, wordCount, userSessions, tollDomain, tollApplication)

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
      magnolifyScalaCheck,
      diffx
    )
  )

lazy val shared = (project in file("stream-processing-shared"))
  .configs(IntegrationTest.extend(Test))
  .enablePlugins(JacocoItPlugin)
  .settings(
    commonSettings,
    integrationTestSettings,
    libraryDependencies ++= Seq(
      scio,
      scioGcp,
      scalaLogging,
      slf4j,
      slf4jJcl,
      logback
    )
  )
  .dependsOn(test % Test)

lazy val wordCount = (project in file("word-count"))
  .settings(commonSettings)
  .dependsOn(
    shared,
    test % Test
  )

lazy val userSessions = (project in file("user-sessions"))
  .settings(commonSettings)
  .dependsOn(
    shared,
    test % Test
  )

lazy val tollDomain = (project in file("toll-domain"))
  .settings(commonSettings)
  .dependsOn(
    shared,
    test % Test
  )

lazy val tollApplication = (project in file("toll-application"))
  .settings(commonSettings)
  .dependsOn(
    shared,
    test % Test,
    tollDomain % "compile->compile;test->test"
  )

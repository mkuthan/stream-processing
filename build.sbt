import sbt._

import Dependencies._
import Settings._

lazy val root = (project in file("."))
  .settings(
    name := "stream-processing",
    commonSettings
  ).aggregate(shared, wordCount, userSessions, tollApplication, tollDomain, tollInfrastructure)

lazy val shared = (project in file("shared"))
  .configs(IntegrationTest)
  .enablePlugins(JacocoItPlugin)
  .settings(
    commonSettings,
    integrationTestSettings,
    libraryDependencies ++= Seq(
      scio,
      scioGcp,
      scioTest % Test,
      scioTest % IntegrationTest,
      scalaLogging,
      slf4j,
      slf4jJcl,
      logback,
      scalaTest % Test,
      scalaTest % IntegrationTest
    )
  )

lazy val wordCount = (project in file("word-count"))
  .settings(commonSettings)
  .dependsOn(shared % "compile->compile;test->test")

lazy val userSessions = (project in file("user-sessions"))
  .settings(commonSettings)
  .dependsOn(shared % "compile->compile;test->test")

lazy val tollApplication = (project in file("toll-application"))
  .settings(commonSettings)
  .dependsOn(shared % "compile->compile;test->test", tollDomain, tollInfrastructure)

lazy val tollDomain = (project in file("toll-domain"))
  .settings(commonSettings)
  .dependsOn(shared % "compile->compile;test->test")

lazy val tollInfrastructure = (project in file("toll-infrastructure"))
  .configs(IntegrationTest)
  .enablePlugins(JacocoItPlugin)
  .settings(
    commonSettings,
    integrationTestSettings,
    libraryDependencies ++= Seq(
      json4s,
      json4sExt
    )
  ).dependsOn(shared % "compile->compile;test->test;it->it")

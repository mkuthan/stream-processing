import sbt._

import Dependencies._
import Settings._

lazy val root = (project in file("."))
  .settings(name := "stream-processing")
  .settings(commonSettings)
  .aggregate(shared, wordCount, userSessions, tollApplication, tollDomain, tollInfrastructure)

lazy val shared = (project in file("shared"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest % Test, logback, scalaTest % Test))

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
  .settings(libraryDependencies ++= Seq(scioGcp))
  .dependsOn(shared % "compile->compile;test->test")

lazy val tollInfrastructure = (project in file("toll-infrastructure"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(
    scioGcp,
    json4s,
    json4sExt,
    scioTest % IntegrationTest,
    scalaTest % IntegrationTest,
    googleCloudStorage % IntegrationTest
  ))
  .dependsOn(shared % "compile->compile;test->test")

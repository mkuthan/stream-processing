import sbt._

import Dependencies._
import Settings._

lazy val root = (project in file("."))
  .settings(name := "stream-processing")
  .settings(commonSettings)
  .aggregate(shared, wordCount, userSessions, tollApplication, tollDomain, tollInfrastructure)

lazy val shared = (project in file("shared"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scalaTest))

lazy val wordCount = (project in file("word-count"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scalaTest))
  .dependsOn(shared % "compile->compile;test->test")

lazy val userSessions = (project in file("user-sessions"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scalaTest))
  .dependsOn(shared % "compile->compile;test->test")

lazy val tollApplication = (project in file("toll-application"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scioGcp, scalaTest))
  .dependsOn(shared % "compile->compile;test->test", tollDomain, tollInfrastructure)

lazy val tollDomain = (project in file("toll-domain"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scioGcp, scalaTest))
  .dependsOn(shared % "compile->compile;test->test")

lazy val tollInfrastructure = (project in file("toll-infrastructure"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(scio, scioTest, scioGcp, json4s, json4sExt, scalaTest))
  .dependsOn(shared % "compile->compile;test->test")

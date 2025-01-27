import sbt.*
import sbt.nio.Keys.onChangedBuildSource
import sbt.nio.Keys.ReloadOnSourceChanges
import sbt.Keys.*

import com.github.sbt.jacoco.JacocoKeys.jacocoReportSettings
import com.github.sbt.jacoco.JacocoKeys.JacocoReportFormats
import com.github.sbt.jacoco.JacocoPlugin.autoImport.JacocoReportSettings
import sbtassembly.AssemblyPlugin.autoImport.*
import scalafix.sbt.ScalafixPlugin.autoImport.*

object Settings {
  val commonSettings = Seq(
    scalaVersion := "2.13.15",
    scalacOptions := Seq(
      "-deprecation", // Emit warning and location for usages of deprecated APIs
      "-feature", // Emit warning and location for usages of features that should be imported explicitly
      "-explaintypes", // Explain type errors in more detail
      "-unchecked", // Enable additional warnings where generated code depends on assumptions
      "-Xlint", // Enable recommended warnings
      "-Wdead-code", // Warn when dead code is identified
      "-Wextra-implicit", // Warn when more than one implicit parameter section is defined
      "-Wmacros:both", // Lints code before and after applying a macro
      "-Wnumeric-widen", // Warn when numerics are widened
      "-Woctal-literal", // Warn on obsolete octal syntax
      "-Wunused:imports", // Warn if an import selector is not referenced
      "-Wunused:patvars", // Warn if a variable bound in a pattern is unused
      "-Wunused:privates", // Warn if a private member is unused
      "-Wunused:locals", // Warn if a local definition is unused
      "-Wunused:explicits", // Warn if an explicit parameter is unused
      "-Wunused:implicits", // Warn if an implicit parameter is unused
      "-Wvalue-discard", // Warn when non-Unit expression results are unused
      // scio specific
      "-Ymacro-annotations", // required by macros
      "-Xmacro-settings:show-coder-fallback=true" // warn about fallback to Kryo coder
    ),
    // automatically reload the build when source changes are detected
    Global / onChangedBuildSource := ReloadOnSourceChanges,
    // experimental feature to speed up the build
    updateOptions := updateOptions.value.withCachedResolution(true),
    // required by beam-runners-google-cloud-dataflow-java
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    // disable integration tests by default
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow"),
    // use jcl-over-slf4j bridge instead of common-logging
    excludeDependencies += "commons-logging" % "commons-logging",
    // pin jackson
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.4",
    // don't check the version compatibility of this library
    libraryDependencySchemes += "com.github.luben" % "zstd-jni" % VersionScheme.Always,
    // enable XML report for codecov
    jacocoReportSettings := JacocoReportSettings()
      .withFormats(JacocoReportFormats.XML, JacocoReportFormats.HTML),
    // scalafix
    ThisBuild / semanticdbEnabled := true,
    ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
  )

  val assemblySettings = Seq(
    assembly / assemblyJarName := "assembly.jar",
    assembly / assemblyMergeStrategy := {
      case s if s.endsWith(".class")         => MergeStrategy.last
      case s if s.endsWith(".proto")         => MergeStrategy.last
      case s if s.endsWith(".kotlin_module") => MergeStrategy.last
      case s if s.endsWith(".properties")    => MergeStrategy.filterDistinctLines
      case s if s.endsWith(".txt")           => MergeStrategy.discard
      case PathList("META-INF", "gradle", "incremental.annotation.processors") =>
        MergeStrategy.discard
      case PathList("META-INF", "kotlin-project-structure-metadata.json") =>
        MergeStrategy.discard
      case PathList("git.properties") =>
        MergeStrategy.discard
      case PathList("commonMain", _*) =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}

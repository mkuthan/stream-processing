import sbt._
import sbt.nio.Keys._
import sbt.Keys._

import com.github.sbt.jacoco.JacocoKeys.jacocoReportSettings
import com.github.sbt.jacoco.JacocoKeys.JacocoReportFormats
import com.github.sbt.jacoco.JacocoPlugin.autoImport.JacocoReportSettings

object Settings {
  val commonSettings =
    Seq(
      scalaVersion := "2.13.10",
      scalacOptions := Seq(
        "-deprecation",
        "-feature",
        "-unchecked",
        "-Ymacro-annotations", // required by Scio macros
        "-Xmacro-settings:show-coder-fallback=true" // warn about fallback to Kryo coder
      ),
      // automatically reload the build when source changes are detected
      Global / onChangedBuildSource := ReloadOnSourceChanges,
      // jacoco
      jacocoReportSettings := JacocoReportSettings()
        .withFormats(JacocoReportFormats.XML, JacocoReportFormats.HTML)
    )
}

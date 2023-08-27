package org.mkuthan.streamprocessing.infrastructure.diagnostic

import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

case class DiagnosticConfiguration(
    windowDuration: Duration = Duration.standardMinutes(10),
    maxRecords: Int = 1_000_000,
    allowedLateness: Duration = Duration.ZERO
) {
  import DiagnosticConfiguration._

  lazy val windowOptions: WindowOptions = createWindowOptions(allowedLateness, maxRecords)

  def withWindowDuration(windowDuration: Duration): DiagnosticConfiguration =
    copy(windowDuration = windowDuration)

  def withMaxRecords(maxRecords: Int): DiagnosticConfiguration =
    copy(maxRecords = maxRecords)

  def withAllowedLateness(allowedLateness: Duration): DiagnosticConfiguration =
    copy(allowedLateness = allowedLateness)
}

object DiagnosticConfiguration {
  private def createWindowOptions(allowedLateness: Duration, elementCount: Int): WindowOptions =
    WindowOptions(
      trigger = Repeatedly.forever(
        AfterFirst.of(
          AfterWatermark.pastEndOfWindow(),
          AfterPane.elementCountAtLeast(elementCount)
        )
      ),
      allowedLateness = allowedLateness,
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
    )
}

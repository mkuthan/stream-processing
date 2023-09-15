package org.mkuthan.streamprocessing.shared.scio

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration

case class FixedWindowConfiguration(
    windowDuration: Duration = Duration.standardMinutes(10),
    maxRecords: Int = 1_000_000,
    allowedLateness: Duration = Duration.ZERO
) {
  import FixedWindowConfiguration._

  lazy val windowOptions: WindowOptions = createWindowOptions(allowedLateness, maxRecords)

  def withWindowDuration(duration: Duration): FixedWindowConfiguration =
    copy(windowDuration = duration)

  def withMaxRecords(maxRecords: Int): FixedWindowConfiguration =
    copy(maxRecords = maxRecords)

  def withAllowedLateness(duration: Duration): FixedWindowConfiguration =
    copy(allowedLateness = duration)
}

object FixedWindowConfiguration {
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

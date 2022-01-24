package org.mkuthan.examples.streaming.wordcount

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object BeamWordCount {

  def wordCountInFixedWindow(lines: SCollection[String], windowDuration: Duration, allowedLateness: Duration): SCollection[(String, Long)] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES, // count late events
      trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()), // default
      closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY, // default
      timestampCombiner = TimestampCombiner.END_OF_WINDOW, // default
      onTimeBehavior = OnTimeBehavior.FIRE_ALWAYS // default
    )

    lines
      .flatMap(line => line.split("\\s+"))
      .withFixedWindows(duration = windowDuration, options = windowOptions)
      .countByValue
  }

}

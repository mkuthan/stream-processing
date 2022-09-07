package org.mkuthan.examples.streaming.wordcount

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object BeamWordCount {

  def wordCountInFixedWindow(
      lines: SCollection[String],
      windowDuration: Duration,
      allowedLateness: Duration = Duration.ZERO,
      accumulationMode: AccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      timestampCombiner: TimestampCombiner = TimestampCombiner.END_OF_WINDOW
  ): SCollection[(String, Long)] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      accumulationMode = accumulationMode,
      timestampCombiner = timestampCombiner
    )
    lines
      .flatMap(line => line.split("\\s+"))
      .withFixedWindows(duration = windowDuration, options = windowOptions)
      .countByValue
  }

  def unusedMethod(): Unit = {
    println("foo")
  }

}

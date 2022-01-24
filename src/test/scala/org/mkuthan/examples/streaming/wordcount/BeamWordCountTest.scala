package org.mkuthan.examples.streaming.wordcount

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.joda.time.Duration
import org.mkuthan.examples.streaming.beam.TimestampedMatchers
import org.mkuthan.examples.streaming.beam._

class BeamWordCountTest extends PipelineSpec with TimestampedMatchers {

  "Words" should "be counted in fixed window" in runWithContext { sc =>
    val windowDuration = Duration.standardMinutes(10L)
    val allowedLateness = Duration.standardMinutes(1)

    val words = testStreamOf[String]
      // on-time events
      .addElementsAt("12:00:00", "foo bar")
      .addElementsAt("12:05:00", "bar baz")
      .advanceWatermarkTo("12:10:00")
      // late event (under allowed lateness)
      .addElementsAt("12:09:00", "foo")
      // on-time event
      .addElementsAt("12:10:00", "foo bar")
      .advanceWatermarkTo("12:11:00")
      // late event (discarded)
      .addElementsAt("12:09:00", "foo")
      .advanceWatermarkToInfinity()

    val results = BeamWordCount
      .wordCountInFixedWindow(sc.testStream(words), windowDuration, allowedLateness)
      .withTimestamp

    results should inOnTimePane("12:00:00", "12:10:00") {
      containInAnyOrderAtWindowTime(
        "12:10:00", Seq(
          ("foo", 1L),
          ("bar", 2L),
          ("baz", 1L)
        ))
    }

    results should inLatePane("12:00:00", "12:10:00") {
      containSingleValueAtWindowTime("12:10:00", ("foo", 2L))
    }

    results should inOnTimePane("12:10:00", "12:20:00") {
      containInAnyOrderAtWindowTime(
        "12:20:00", Seq(
          ("foo", 1L),
          ("bar", 1L)
        ))
    }

    results should inLatePane("12:10:00", "12:20:00") {
      beEmpty
    }
  }
}

package org.mkuthan.examples.streaming.beam

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.joda.time.Duration

class WordCountTest extends PipelineSpec with TimestampedMatchers {

  import TestImplicits._

  "Words" should "be counted in fixed window" in runWithContext { sc =>
    val windowDuration = Duration.standardMinutes(10L)
    val allowedLateness = Duration.standardMinutes(1)

    val words = testStreamOf[String]
      .addElementsAt("12:00:00", "foo bar")
      .addElementsAt("12:05:00", "bar baz")
      .advanceWatermarkTo("12:10:00")
      .addElementsAt("12:09:00", "foo") // late event in the 12:00-12:10 window
      .addElementsAt("12:10:00", "foo bar") // non-late event in the 12:10-12:20 window
      .advanceWatermarkTo("12:11:00")
      .addElementsAt("12:09:00", "foo") // discarded late event
      .advanceWatermarkToInfinity()

    val results = WordCount
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

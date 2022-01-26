package org.mkuthan.examples.streaming.wordcount

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.mkuthan.examples.streaming.beam.TimestampedMatchers
import org.mkuthan.examples.streaming.beam._

class BeamWordCountTest extends PipelineSpec with TimestampedMatchers {

  import BeamWordCount._

  private val DefaultWindowDuration = Duration.standardMinutes(1L)

  "Words aggregate" should "be empty for empty stream" in runWithContext { sc =>
    val words = testStreamOf[String].advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration)

    results should beEmpty
  }

  "Words" should "be aggregated into single fixed window" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration)

    results.withTimestamp should containInAnyOrderAtWindowTime(Seq(
      ("00:01:00", ("foo", 1L)),
      ("00:01:00", ("bar", 1L)),
      ("00:01:00", ("baz", 2L)),
    ))
  }

  it should "be aggregated into consecutive fixed windows" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .addElementsAt("00:01:00", "foo bar")
      .addElementsAt("00:01:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration)

    results.withTimestamp should containInAnyOrderAtWindowTime(Seq(
      ("00:01:00", ("foo", 1L)),
      ("00:01:00", ("bar", 1L)),
      ("00:01:00", ("baz", 2L)),
      ("00:02:00", ("foo", 2L)),
      ("00:02:00", ("bar", 2L)),
    ))
  }

  it should "be aggregated into non-consecutive fixed windows" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .addElementsAt("00:02:00", "foo bar")
      .addElementsAt("00:02:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration)

    results.withTimestamp should containInAnyOrderAtWindowTime(Seq(
      ("00:01:00", ("foo", 1L)),
      ("00:01:00", ("bar", 1L)),
      ("00:01:00", ("baz", 2L)),
      ("00:03:00", ("foo", 2L)),
      ("00:03:00", ("bar", 2L)),
    ))

    results.withTimestamp should inOnTimePane("00:01:00", "00:02:00") {
      beEmpty
    }
  }

  "Late words" should "be silently dropped" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAt("00:00:40", "foo") // late event
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration)

    results.withTimestamp should containInAnyOrderAtWindowTime(Seq(
      ("00:01:00", ("foo", 1L)),
      ("00:01:00", ("bar", 1L)),
      ("00:01:00", ("baz", 2L)),
    ))
  }

  "Late words under allowed lateness" should "be aggregated in late pane" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAt("00:00:40", "foo foo") // late event under allowed lateness
      .advanceWatermarkToInfinity()

    val allowedLateness = Duration.standardSeconds(30)
    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration, allowedLateness)

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containInAnyOrderAtWindowTime(Seq(
        ("00:01:00", ("foo", 1L)),
        ("00:01:00", ("bar", 1L)),
        ("00:01:00", ("baz", 2L)),
      ))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containSingleValueAtWindowTime(
        "00:01:00", ("foo", 2L)
      )
    }
  }

  "Late words under allowed lateness" should "be aggregated and accumulated in late pane" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAt("00:00:00", "foo bar")
      .addElementsAt("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAt("00:00:40", "foo foo") // late event under allowed lateness
      .advanceWatermarkToInfinity()

    val allowedLateness = Duration.standardSeconds(30)
    val accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    val results = wordCountInFixedWindow(sc.testStream(words), DefaultWindowDuration, allowedLateness, accumulationMode)

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containInAnyOrderAtWindowTime(Seq(
        ("00:01:00", ("foo", 1L)),
        ("00:01:00", ("bar", 1L)),
        ("00:01:00", ("baz", 2L))
      ))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containSingleValueAtWindowTime(
        "00:01:00", ("foo", 3L)
      )
    }
  }
}

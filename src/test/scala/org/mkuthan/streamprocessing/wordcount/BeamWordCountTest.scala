package org.mkuthan.streamprocessing.wordcount

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.mkuthan.streamprocessing.beam.TimestampedMatchers
import org.mkuthan.streamprocessing.beam._

class BeamWordCountTest extends PipelineSpec with TimestampedMatchers {

  import BeamWordCount._

  private val OneMinute = Duration.standardMinutes(1L)

  "Words aggregate" should "be empty for empty stream" in runWithContext { sc =>
    val words = testStreamOf[String].advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), OneMinute)

    results should beEmpty
  }

  "Words" should "be aggregated into single fixed window" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), OneMinute)

    results.withTimestamp should containInAnyOrderAtTime(Seq(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L))
    ))
  }

  "Words" should "be aggregated into single fixed window with latest timestamp" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testStream(words),
      OneMinute,
      timestampCombiner = TimestampCombiner.LATEST
    )

    results.withTimestamp should containInAnyOrderAtTime(Seq(
      ("00:00:00", ("foo", 1L)),
      ("00:00:00", ("bar", 1L)),
      ("00:00:30", ("baz", 2L))
    ))
  }

  "Words" should "be aggregated into consecutive fixed windows" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .addElementsAtTime("00:01:00", "foo bar")
      .addElementsAtTime("00:01:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), OneMinute)

    results.withTimestamp should containInAnyOrderAtTime(Seq(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L)),
      ("00:01:59.999", ("foo", 2L)),
      ("00:01:59.999", ("bar", 2L))
    ))
  }

  "Words" should "be aggregated into non-consecutive fixed windows" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .addElementsAtTime("00:02:00", "foo bar")
      .addElementsAtTime("00:02:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), OneMinute)

    results.withTimestamp should containInAnyOrderAtTime(Seq(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L)),
      ("00:02:59.999", ("foo", 2L)),
      ("00:02:59.999", ("bar", 2L))
    ))

    results.withTimestamp should inWindow("00:01:00", "00:02:00") {
      beEmpty
    }
  }

  "Late words" should "be silently dropped" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo") // late event
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testStream(words), OneMinute)

    results.withTimestamp should containInAnyOrderAtTime(Seq(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L))
    ))
  }

  "Late words within allowed lateness" should "be aggregated in late pane" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo foo") // late event within allowed lateness
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testStream(words),
      OneMinute,
      allowedLateness = Duration.standardSeconds(30)
    )

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containInAnyOrderAtTime(Seq(
        ("00:00:59.999", ("foo", 1L)),
        ("00:00:59.999", ("bar", 1L)),
        ("00:00:59.999", ("baz", 2L))
      ))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containSingleValueAtTime(
        "00:00:59.999",
        ("foo", 2L)
      )
    }
  }

  "Late words within allowed lateness" should "be aggregated and accumulated in late pane" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo foo") // late event within allowed lateness
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testStream(words),
      OneMinute,
      allowedLateness = Duration.standardSeconds(30),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containInAnyOrderAtTime(Seq(
        ("00:00:59.999", ("foo", 1L)),
        ("00:00:59.999", ("bar", 1L)),
        ("00:00:59.999", ("baz", 2L))
      ))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containSingleValueAtTime(
        "00:00:59.999",
        ("foo", 3L)
      )
    }
  }
}

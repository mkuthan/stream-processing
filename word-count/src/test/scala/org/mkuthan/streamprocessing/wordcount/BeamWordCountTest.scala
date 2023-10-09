package org.mkuthan.streamprocessing.wordcount

import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import org.joda.time.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

final class BeamWordCountTest extends AnyFlatSpec with Matchers with TestScioContext {

  import BeamWordCount._

  private val OneMinute = Duration.standardMinutes(1L)

  "Words aggregate" should "be empty for empty stream" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String].advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testUnbounded(words), OneMinute)

    results should beEmpty
  }

  "Words" should "be aggregated into single fixed window" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testUnbounded(words), OneMinute)

    results.withTimestamp should containElementsAtTime(
      "00:00:59.999",
      ("foo", 1L),
      ("bar", 1L),
      ("baz", 2L)
    )
  }

  "Words" should "be aggregated into single fixed window with latest timestamp" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testUnbounded(words),
      OneMinute,
      timestampCombiner = TimestampCombiner.LATEST
    )

    results.withTimestamp should containElementsAtTime(
      ("00:00:00", ("foo", 1L)),
      ("00:00:00", ("bar", 1L)),
      ("00:00:30", ("baz", 2L))
    )
  }

  "Words" should "be aggregated into consecutive fixed windows" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .addElementsAtTime("00:01:00", "foo bar")
      .addElementsAtTime("00:01:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testUnbounded(words), OneMinute)

    results.withTimestamp should containElementsAtTime(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L)),
      ("00:01:59.999", ("foo", 2L)),
      ("00:01:59.999", ("bar", 2L))
    )
  }

  "Words" should "be aggregated into non-consecutive fixed windows" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .addElementsAtTime("00:02:00", "foo bar")
      .addElementsAtTime("00:02:30", "bar foo")
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testUnbounded(words), OneMinute)

    results.withTimestamp should containElementsAtTime(
      ("00:00:59.999", ("foo", 1L)),
      ("00:00:59.999", ("bar", 1L)),
      ("00:00:59.999", ("baz", 2L)),
      ("00:02:59.999", ("foo", 2L)),
      ("00:02:59.999", ("bar", 2L))
    )

    results.withTimestamp should inWindow("00:01:00", "00:02:00") {
      beEmpty
    }
  }

  "Late words" should "be silently dropped" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo") // late event
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(sc.testUnbounded(words), OneMinute)

    results.withTimestamp should containElementsAtTime(
      "00:00:59.999",
      ("foo", 1L),
      ("bar", 1L),
      ("baz", 2L)
    )
  }

  "Late words within allowed lateness" should "be aggregated in late pane" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo foo") // late event within allowed lateness
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testUnbounded(words),
      OneMinute,
      allowedLateness = Duration.standardSeconds(30)
    )

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containElementsAtTime(
        "00:00:59.999",
        ("foo", 1L),
        ("bar", 1L),
        ("baz", 2L)
      )
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containElementsAtTime(
        "00:00:59.999",
        ("foo", 2L)
      )
    }
  }

  "Late words within allowed lateness" should "be aggregated and accumulated in late pane" in runWithScioContext { sc =>
    val words = unboundedTestCollectionOf[String]
      .addElementsAtTime("00:00:00", "foo bar")
      .addElementsAtTime("00:00:30", "baz baz")
      .advanceWatermarkTo("00:01:00")
      .addElementsAtTime("00:00:40", "foo foo") // late event within allowed lateness
      .advanceWatermarkToInfinity()

    val results = wordCountInFixedWindow(
      sc.testUnbounded(words),
      OneMinute,
      allowedLateness = Duration.standardSeconds(30),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )

    results.withTimestamp should inOnTimePane("00:00:00", "00:01:00") {
      containElementsAtTime("00:00:59.999", ("foo", 1L), ("bar", 1L), ("baz", 2L))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:01:00") {
      containElementsAtTime("00:00:59.999", ("foo", 3L))
    }
  }
}

package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.mkuthan.examples.streaming.beam.TimestampedMatchers
import org.mkuthan.examples.streaming.beam._

class BeamUserSessionsTest extends PipelineSpec with TimestampedMatchers {

  import BeamUserSessions._

  private val DefaultGapDuration = Duration.standardMinutes(10L)

  "Visit without actions" should "create empty session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)].advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results should beEmpty
  }

  "Single visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:00", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("jack", Iterable("open app", "close app")))
    }
  }

  "Two visits" should "be aggregated into two simultaneous sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:00:00", ("ben", "open app"))
      .addElementsAtTime("00:01:00", ("jack", "close app"))
      .addElementsAtTime("00:01:30", ("ben", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("jack", Iterable("open app", "close app")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("ben", Iterable("open app", "close app")))
    }
  }

  "Unordered visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:01:00", ("jack", "close app"))
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("jack", Iterable("open app", "close app")))
    }
  }

  "Continuous visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "show product"))
      .addElementsAtTime("00:03:00", ("jack", "add to cart"))
      .addElementsAtTime("00:09:30", ("jack", "checkout"))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime(
        "00:23:09.999",
        ("jack", Iterable("open app", "show product", "add to cart", "checkout", "close app")))
    }
  }

  "Interrupted visit" should "be aggregated into two sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "show product"))
      .addElementsAtTime("00:03:00", ("jack", "add to cart"))
      .addElementsAtTime("00:13:00", ("jack", "checkout"))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:13:00") {
      containSingleValueAtTime(
        "00:12:59.999",
        ("jack", Iterable("open app", "show product", "add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:13:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("jack", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event" should "be aggregated but late event should be silently dropped" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("jack", "add to cart")) // late event
      .addElementsAtTime("00:09:30", ("jack", "checkout"))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("jack", Iterable("open app", "show product")))
    }

    results.withTimestamp should inWindow("00:00:00", "00:13:00") {
      beEmpty
    }

    results.withTimestamp should inOnTimePane("00:09:30", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("jack", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event within allowed lateness" should "be aggregated also into session in late pane" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("jack", "add to cart")) // late event within allowed lateness
      .addElementsAtTime("00:09:30", ("jack", "checkout"))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      allowedLateness = Duration.standardMinutes(5))

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("jack", Iterable("open app", "show product")))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:13:00") {
      containSingleValueAtTime("00:12:59.999", ("jack", Iterable("add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("jack", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event within allowed lateness" should "be aggregated and accumulated also into session in late pane" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("jack", "add to cart")) // late event within allowed lateness
      .addElementsAtTime("00:09:30", ("jack", "checkout"))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      allowedLateness = Duration.standardMinutes(5),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("jack", Iterable("open app", "show product")))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:13:00") {
      containSingleValueAtTime("00:12:59.999", ("jack", Iterable("open app", "show product", "add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("jack", Iterable("open app", "show product", "add to cart", "checkout", "close app")))
    }
  }

  "Visit" should "be aggregated with speculative results in early panes" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("jack", "open app"))
      .addElementsAtTime("00:01:30", ("jack", "open product"))
      .addElementsAtTime("00:03:00", ("jack", "add to cart"))
      .advanceProcessingTime(Duration.standardMinutes(6))
      .addElementsAtTime("00:09:30", ("jack", "checkout"))
      .advanceProcessingTime(Duration.standardMinutes(6))
      .addElementsAtTime("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val trigger = Repeatedly.forever(
      AfterWatermark
        .pastEndOfWindow()
        .withEarlyFirings(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5))
        ))

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES,
      timestampCombiner = TimestampCombiner.EARLIEST,
      trigger = trigger)

    results.withTimestamp should inEarlyPane("00:00:00", "00:13:00") {
      containSingleValueAtTime(
        "00:00:00.000",
        ("jack", Iterable("open app", "open product", "add to cart")))
    }

    results.withTimestamp should inEarlyPane("00:00:00", "00:19:30") {
      containSingleValueAtTime(
        "00:09:30.000",
        ("jack", Iterable("open app", "open product", "add to cart", "checkout")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime(
        "00:13:10.000",
        ("jack", Iterable("open app", "open product", "add to cart", "checkout", "close app")))
    }
  }
}

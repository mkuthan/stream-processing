package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.mkuthan.examples.streaming.beam.TimestampedMatchers
import org.mkuthan.examples.streaming.beam._

class BeamUserSessionsTest extends PipelineSpec with TimestampedMatchers {

  import BeamUserSessions._

  private val OneMinute = Duration.standardMinutes(1L)
  private val DefaultGapDuration = Duration.standardMinutes(10L)

  "Visit without actions" should "create empty session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)].advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results should beEmpty
  }

  "Single visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:00", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("joe", Iterable("open app", "close app")))
    }
  }

  "Two visits" should "be aggregated into two simultaneous sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:00:00", ("ben", "open app"))
      .addElementsAtTime("00:01:00", ("joe", "close app"))
      .addElementsAtTime("00:01:30", ("ben", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("joe", Iterable("open app", "close app")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("ben", Iterable("open app", "close app")))
    }
  }

  "Unordered visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:01:00", ("joe", "close app"))
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("joe", Iterable("open app", "close app")))
    }
  }

  "Continuous visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:30", ("joe", "show product"))
      .addElementsAtTime("00:03:00", ("joe", "add to cart"))
      .addElementsAtTime("00:09:30", ("joe", "checkout"))
      .addElementsAtTime("00:13:10", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime(
        "00:23:09.999",
        ("joe", Iterable("open app", "show product", "add to cart", "checkout", "close app")))
    }
  }

  "Interrupted visit" should "be aggregated into two sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:30", ("joe", "show product"))
      .addElementsAtTime("00:03:00", ("joe", "add to cart"))
      .addElementsAtTime("00:13:00", ("joe", "checkout"))
      .addElementsAtTime("00:13:10", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:13:00") {
      containSingleValueAtTime(
        "00:12:59.999",
        ("joe", Iterable("open app", "show product", "add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:13:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("joe", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event" should "be aggregated but late event should be silently dropped" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:30", ("joe", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("joe", "add to cart")) // late event
      .addElementsAtTime("00:09:30", ("joe", "checkout"))
      .addElementsAtTime("00:13:10", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("joe", Iterable("open app", "show product")))
    }

    results.withTimestamp should inWindow("00:00:00", "00:13:00") {
      beEmpty
    }

    results.withTimestamp should inOnTimePane("00:09:30", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("joe", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event within allowed lateness" should "be aggregated also into session in late pane" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:30", ("joe", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("joe", "add to cart")) // late event within allowed lateness
      .addElementsAtTime("00:09:30", ("joe", "checkout"))
      .addElementsAtTime("00:13:10", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      allowedLateness = Duration.standardMinutes(5))

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("joe", Iterable("open app", "show product")))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:13:00") {
      containSingleValueAtTime("00:12:59.999", ("joe", Iterable("add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("joe", Iterable("checkout", "close app")))
    }
  }

  "Visit with late event within allowed lateness" should "be aggregated and accumulated also into session in late pane" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "open app"))
      .addElementsAtTime("00:01:30", ("joe", "show product"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAtTime("00:03:00", ("joe", "add to cart")) // late event within allowed lateness
      .addElementsAtTime("00:09:30", ("joe", "checkout"))
      .addElementsAtTime("00:13:10", ("joe", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      allowedLateness = Duration.standardMinutes(5),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtTime("00:11:29.999", ("joe", Iterable("open app", "show product")))
    }

    results.withTimestamp should inLatePane("00:00:00", "00:13:00") {
      containSingleValueAtTime("00:12:59.999", ("joe", Iterable("open app", "show product", "add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtTime("00:23:09.999", ("joe", Iterable("open app", "show product", "add to cart", "checkout", "close app")))
    }
  }

  "Visit" should "be aggregated speculatively on every minute, on-time, and lately on every element" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAtTime("00:00:00", ("joe", "0")).advanceProcessingTime(OneMinute)
      .addElementsAtTime("00:01:00", ("joe", "1")).advanceProcessingTime(OneMinute)
      .addElementsAtTime("00:02:00", ("joe", "2")).advanceProcessingTime(OneMinute)
      .addElementsAtTime("00:03:00", ("joe", "3")).advanceProcessingTime(OneMinute)
      .addElementsAtTime("00:04:00", ("joe", "4")).advanceProcessingTime(OneMinute)
      .addElementsAtTime("00:05:00", ("joe", "5")).advanceProcessingTime(OneMinute)
      .advanceWatermarkTo("00:20:00") // more than 00:05:00 + 10 minutes of gap
      .addElementsAtTime("00:06:00", ("joe", "6")) // late event within allowed lateness
      .addElementsAtTime("00:07:00", ("joe", "7")) // late event within allowed lateness
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES,
      allowedLateness = Duration.standardMinutes(10),
      trigger = AfterWatermark
        .pastEndOfWindow()
        .withEarlyFirings(
          AfterProcessingTime.pastFirstElementInPane().plusDelayOf(OneMinute))
        .withLateFirings(
          AfterPane.elementCountAtLeast(1))
    )

    results.withPaneInfo.withWindow.debug()

    results.withTimestamp should inEarlyPane("00:00:00", "00:11:00") {
      containSingleValueAtTime("00:10:59.999", ("joe", Iterable("0", "1")))
    }
    results.withTimestamp should inEarlyPane("00:00:00", "00:13:00") {
      containSingleValueAtTime("00:12:59.999", ("joe", Iterable("0", "1", "2", "3")))
    }
    results.withTimestamp should inEarlyPane("00:00:00", "00:15:00") {
      containSingleValueAtTime("00:14:59.999", ("joe", Iterable("0", "1", "2", "3", "4", "5")))
    }
    results.withTimestamp should inOnTimePane("00:00:00", "00:15:00") {
      containSingleValueAtTime("00:14:59.999", ("joe", Iterable("0", "1", "2", "3", "4", "5")))
    }
    results.withTimestamp should inLatePane("00:00:00", "00:16:00") {
      containSingleValueAtTime("00:15:59.999", ("joe", Iterable("0", "1", "2", "3", "4", "5", "6")))
    }
    results.withTimestamp should inLatePane("00:00:00", "00:17:00") {
      containSingleValueAtTime("00:16:59.999", ("joe", Iterable("0", "1", "2", "3", "4", "5", "6", "7")))
    }
  }
}

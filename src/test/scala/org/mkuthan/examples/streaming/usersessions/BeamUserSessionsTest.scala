package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf
import org.joda.time.Duration
import org.mkuthan.examples.streaming.beam.TimestampedMatchers
import org.mkuthan.examples.streaming.beam._

class BeamUserSessionsTest extends PipelineSpec with TimestampedMatchers {

  import BeamUserSessions._

  private val DefaultGapDuration = Duration.standardMinutes(10L)

  "Session" should "be empty for empty stream" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)].advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results should beEmpty
  }

  "Short visit" should "be aggregated into single session" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:01:00", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtWindowTime("00:11:00", ("jack", Iterable("open app", "close app")))
    }
  }

  "Short visit from two clients" should "be aggregated into two simultaneous sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:00:00", ("ben", "open app"))
      .addElementsAt("00:01:00", ("jack", "close app"))
      .addElementsAt("00:01:30", ("ben", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:00") {
      containSingleValueAtWindowTime("00:11:00", ("jack", Iterable("open app", "close app")))
    }

    results.withTimestamp should inOnTimePane("00:00:00", "00:11:30") {
      containSingleValueAtWindowTime("00:11:30", ("ben", Iterable("open app", "close app")))
    }
  }

  "Long but continuous visit" should "be aggregated into single sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:01:00", ("jack", "search product"))
      .addElementsAt("00:01:30", ("jack", "open product"))
      .addElementsAt("00:03:00", ("jack", "add to cart"))
      .addElementsAt("00:09:30", ("jack", "checkout"))
      .addElementsAt("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:23:10") {
      containSingleValueAtWindowTime(
        "00:23:10",
        ("jack", Iterable("open app", "search product", "open product", "add to cart", "checkout", "close app")))
    }
  }

  "Long interrupted visit" should "be aggregated into two sessions" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:01:00", ("jack", "search product"))
      .addElementsAt("00:01:30", ("jack", "open product"))
      .addElementsAt("00:03:00", ("jack", "add to cart"))
      .addElementsAt("00:13:00", ("jack", "checkout"))
      .addElementsAt("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:13:00") {
      containSingleValueAtWindowTime(
        "00:13:00",
        ("jack", Iterable("open app", "search product", "open product", "add to cart")))
    }

    results.withTimestamp should inOnTimePane("00:13:00", "00:23:10") {
      containSingleValueAtWindowTime("00:23:10", ("jack", Iterable("checkout", "close app")))
    }
  }

  "Late event" should "not close the gap and two sessions are materialized" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:01:00", ("jack", "search product"))
      .addElementsAt("00:01:30", ("jack", "open product"))
      .addElementsAt("00:03:00", ("jack", "add to cart"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAt("00:09:30", ("jack", "checkout"))
      .addElementsAt("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(sc.testStream(userActions), DefaultGapDuration)

    results.withTimestamp should inOnTimePane("00:00:00", "00:13:00") {
      containSingleValueAtWindowTime("00:13:00", ("jack", Iterable("open app", "search product", "open product", "add to cart")))
    }

    // Why session window starts at 00:09:30 if watermark has been advanced to 00:13:00? I would expect dropped "checkout" due to lateness.
    results.withTimestamp should inOnTimePane("00:09:30", "00:23:10") {
      containSingleValueAtWindowTime("00:23:10", ("jack", Iterable("checkout", "close app")))
    }
  }

  "Late event" should "TODO" in runWithContext { sc =>
    val userActions = testStreamOf[(User, Action)]
      .addElementsAt("00:00:00", ("jack", "open app"))
      .addElementsAt("00:01:00", ("jack", "search product"))
      .addElementsAt("00:01:30", ("jack", "open product"))
      .addElementsAt("00:03:00", ("jack", "add to cart"))
      .advanceWatermarkTo("00:13:00")
      .addElementsAt("00:09:30", ("jack", "checkout"))
      .addElementsAt("00:13:10", ("jack", "close app"))
      .advanceWatermarkToInfinity()

    val results = activitiesInSessionWindow(
      sc.testStream(userActions),
      DefaultGapDuration,
      allowedLateness = Duration.standardMinutes(5))

    results.withTimestamp should inOnTimePane("00:00:00", "00:13:00") {
      containSingleValueAtWindowTime("00:13:00", ("jack", Iterable("open app", "search product", "open product", "add to cart")))
    }

    //    results.withTimestamp should inWindow("00:09:30", "00:23:10") {
    //      containInAnyOrderAtWindowTime(Seq(
    //        ("00:23:10", ("jack", Iterable("checkout", "close app")))
    //      ))
    //    }
  }

  // TODO: late event that fill the gap (discarded vs. accumulated)

  // TODO: speculative early results
}

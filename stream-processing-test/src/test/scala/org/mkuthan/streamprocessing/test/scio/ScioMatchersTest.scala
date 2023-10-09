package org.mkuthan.streamprocessing.test.scio

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._

class ScioMatchersTest extends AnyFlatSpec with Matchers with TestScioContext {
  behavior of "ScioMatchers"

  it should "match single element" in runWithScioContext { sc =>
    val input = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("first")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input)
    results should containElements("first")
  }

  it should "match single element at time" in runWithScioContext { sc =>
    val anyTime = Instant.parse("2000-01-01T00:00:00.000Z")
    val input = boundedTestCollectionOf[String]
      .addElementsAtTime(anyTime, "first")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input).withTimestamp
    results should containElementsAtTime(anyTime, "first")
    results should containElementsAtTime(anyTime.toString, "first")
  }

  it should "match many elements" in runWithScioContext { sc =>
    val input = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("first", "second", "third")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input)
    results should containElements("first", "second", "third")
    results should containElements("third", "second", "first")
  }

  it should "match many elements at time" in runWithScioContext { sc =>
    val anyTime = Instant.parse("2000-01-01T00:00:00.000Z")

    val input = boundedTestCollectionOf[String]
      .addElementsAtTime(anyTime, "first", "second", "third")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input).withTimestamp
    results should containElementsAtTime(anyTime, "first", "second", "third")
    results should containElementsAtTime(anyTime, "third", "second", "first")
    results should containElementsAtTime(anyTime.toString, "first", "second", "third")
    results should containElementsAtTime(anyTime.toString, "third", "second", "first")
  }

  it should "match many elements at different times" in runWithScioContext { sc =>
    val time1 = "2000-01-01T00:00:00.000Z"
    val time2 = "2000-01-02T00:00:00.000Z"

    val input = boundedTestCollectionOf[String]
      .addElementsAtTime(time1, "first", "second", "third")
      .addElementsAtTime(time2, "fourth", "fifth")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input).withTimestamp
    results should containElementsAtTime(
      (time1, "first"),
      (time1, "second"),
      (time1, "third"),
      (time2, "fourth"),
      (time2, "fifth")
    )

    val time1Instant = Instant.parse(time1)
    val time2Instant = Instant.parse(time2)

    results should containElementsAtTime(
      (time1Instant, "first"),
      (time1Instant, "second"),
      (time1Instant, "third"),
      (time2Instant, "fourth"),
      (time2Instant, "fifth")
    )
  }
}

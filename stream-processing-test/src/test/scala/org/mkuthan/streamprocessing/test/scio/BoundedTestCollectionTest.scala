package org.mkuthan.streamprocessing.test.scio

import org.apache.beam.sdk.transforms.windowing.BoundedWindow

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._

class BoundedTestCollectionTest extends AnyFlatSpec with Matchers with TestScioContext {
  "Builder" should "build BoundedTestCollection" in runWithScioContext { sc =>
    val minTime = BoundedWindow.TIMESTAMP_MIN_VALUE
    val anyTime = Instant.parse("2000-01-01T00:00:00.000Z")

    val input = BoundedTestCollection.builder[String]()
      .addElementsAtMinimumTime("first", "second", "third")
      .addElementsAtTime(anyTime, "fourth", "fifth", "sixth")
      .addElementsAtTime(anyTime.toString, "seventh")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(input).withTimestamp
    results should containElementsAtTime(
      (minTime, "first"),
      (minTime, "second"),
      (minTime, "third"),
      (anyTime, "fourth"),
      (anyTime, "fifth"),
      (anyTime, "sixth"),
      (anyTime, "seventh")
    )
  }

}

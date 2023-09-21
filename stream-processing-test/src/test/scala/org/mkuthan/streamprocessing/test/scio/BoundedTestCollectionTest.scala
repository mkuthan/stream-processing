package org.mkuthan.streamprocessing.test.scio

import org.apache.beam.sdk.transforms.windowing.BoundedWindow

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BoundedTestCollectionTest extends AnyFlatSpec with Matchers with TestScioContext {
  "Builder" should "build BoundedTestCollection" in runWithScioContext { sc =>
    val minTime = BoundedWindow.TIMESTAMP_MIN_VALUE.toString
    val anyTime = "2000-01-01T00:00:00.000Z"

    val boundedTestCollection = BoundedTestCollection.builder[String]()
      .addElementsAtMinimumTime("first", "second", "third")
      .addElementsAtTime(anyTime, "fourth", "fifth", "sixth")
      .addElementsAtTime(Instant.parse(anyTime), "seventh")
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(boundedTestCollection)
    results.withTimestamp should containInAnyOrderAtTime[String](Seq(
      (minTime, "first"),
      (minTime, "second"),
      (minTime, "third"),
      (anyTime, "fourth"),
      (anyTime, "fifth"),
      (anyTime, "sixth"),
      (anyTime, "seventh")
    ))
  }

}

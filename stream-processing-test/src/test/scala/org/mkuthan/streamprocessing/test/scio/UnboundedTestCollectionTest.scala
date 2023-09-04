package org.mkuthan.streamprocessing.test.scio

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UnboundedTestCollectionTest extends AnyFlatSpec with Matchers with TestScioContext {
  "Builder" should "build UnboundedTestCollection" in runWithScioContext { sc =>
    val anyTime = "2000-01-01T00:00:00.000Z"
    val watermarkTime1 = "2000-01-01T01:00:00.000Z"
    val watermarkTime2 = "2000-01-01T02:00:00.000Z"

    val unboundedTestCollection = UnboundedTestCollection.builder[String]()
      .addElementsAtTime(anyTime, "first", "second", "third")
      .addElementsAtTime(Instant.parse(anyTime), "fourth")
      .advanceWatermarkTo(watermarkTime1)
      .addElementsAtWatermarkTime("fifth")
      .advanceWatermarkTo(watermarkTime2)
      .addElementsAtWatermarkTime("sixth", "seventh")
      .advanceWatermarkToInfinity()

    val results = sc.testUnbounded(unboundedTestCollection)
    results.withTimestamp should containInAnyOrderAtTime[String](Seq(
      (anyTime, "first"),
      (anyTime, "second"),
      (anyTime, "third"),
      (anyTime, "fourth"),
      (watermarkTime1, "fifth"),
      (watermarkTime2, "sixth"),
      (watermarkTime2, "seventh")
    ))
  }

}

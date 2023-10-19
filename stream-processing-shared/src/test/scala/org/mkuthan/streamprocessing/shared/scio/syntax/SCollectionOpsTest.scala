package org.mkuthan.streamprocessing.shared.scio.syntax

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class SCollectionOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionSyntax {

  behavior of "SCollection syntax"

  it should "union in global window" in runWithScioContext { sc =>
    val collection1 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("one").advanceWatermarkToInfinity()
    val collection2 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("two", "three").advanceWatermarkToInfinity()
    val collection3 = boundedTestCollectionOf[String]
      .advanceWatermarkToInfinity()

    val results = sc
      .testBounded(collection1)
      .withGlobalWindow()
      .unionInGlobalWindow(
        sc.testBounded(collection2).windowByDays(1),
        sc.testBounded(collection3).windowByMonths(1)
      )

    results should containElements("one", "two", "three")
  }

  it should "map with timestamp" in runWithScioContext { sc =>
    val instant = Instant.parse("2023-09-15T18:19:00Z")
    val element = "any element"
    val collection = boundedTestCollectionOf[String]
      .addElementsAtTime(instant, element)
      .advanceWatermarkToInfinity()

    val results = sc.testBounded(collection)
      .mapWithTimestamp { case (e, i) => e + i.toString }

    results should containElements(s"$element$instant")
  }
}

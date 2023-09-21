package org.mkuthan.streamprocessing.shared.scio.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.boundedTestCollectionOf
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class ScioContextOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with ScioContextSyntax {

  behavior of "SContext syntax"

  it should "union in global window" in runWithScioContext { sc =>
    val collection1 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("one").advanceWatermarkToInfinity()
    val collection2 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("two", "three").advanceWatermarkToInfinity()
    val collection3 = boundedTestCollectionOf[String]
      .advanceWatermarkToInfinity()

    val results = sc.unionInGlobalWindow(
      sc.testBounded(collection1).withGlobalWindow(),
      sc.testBounded(collection2).windowByDays(1),
      sc.testBounded(collection3).windowByMonths(1)
    )
    results should containInAnyOrder(Seq("one", "two", "three"))
  }
}

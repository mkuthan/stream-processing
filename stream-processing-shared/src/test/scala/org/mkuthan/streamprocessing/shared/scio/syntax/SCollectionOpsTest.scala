package org.mkuthan.streamprocessing.shared.scio.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.boundedTestCollectionOf
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class SCollectionOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionSyntax {

  behavior of "SCollection syntax"

  it should "union in global window" in runWithScioContext { sc =>
    val collection1 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("one").build()
    val collection2 = boundedTestCollectionOf[String]
      .addElementsAtMinimumTime("two", "three").build()
    val collection3 = boundedTestCollectionOf[String]
      .build()

    val results = sc
      .testBounded(collection1)
      .withGlobalWindow()
      .unionInGlobalWindow(
        sc.testBounded(collection2).windowByDays(1),
        sc.testBounded(collection3).windowByMonths(1)
      )

    results should containInAnyOrder(Seq("one", "two", "three"))
  }
}

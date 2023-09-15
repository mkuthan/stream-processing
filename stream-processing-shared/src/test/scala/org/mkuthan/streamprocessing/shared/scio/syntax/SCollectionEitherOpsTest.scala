package org.mkuthan.streamprocessing.shared.scio.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.boundedTestCollectionOf
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class SCollectionEitherOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionSyntax {

  behavior of "SCollection syntax"

  it should "unzip Either" in runWithScioContext { sc =>
    val collection = boundedTestCollectionOf[Either[String, String]]
      .addElementsAtMinimumTime(Right("r1"), Left("l1"), Right("r2"), Left("l2"), Right("r3"))
      .build()

    val (right, left) = sc
      .testBounded(collection)
      .unzip

    right should containInAnyOrder(Seq("r1", "r2", "r3"))
    left should containInAnyOrder(Seq("l1", "l2"))
  }
}

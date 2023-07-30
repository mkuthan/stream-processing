package org.mkuthan.streamprocessing.shared.scio.core

import com.spotify.scio.testing._
import com.spotify.scio.testing.SCollectionMatchers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.TestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionMatchers
    with SCollectionSyntax {

  behavior of "Core SCollection syntax"

  it should "unzip Either" in runWithScioContext { sc =>
    val (right, left) = sc
      .parallelize[Either[String, String]](Seq(Right("r1"), Left("l1"), Right("r2"), Left("l2"), Right("r3")))
      .unzip

    right should containInAnyOrder(Seq("r1", "r2", "r3"))
    left should containInAnyOrder(Seq("l1", "l2"))
  }

  it should "union in global window" in runWithScioContext { sc =>
    val boundedCollection = sc.parallelize(Seq("one", "two", "three"))

    val unboundedCollection = testStreamOf[String]
      .addElements("four", "five")
      .advanceWatermarkToInfinity()

    val results = boundedCollection.unionInGlobalWindow(sc.testStream(unboundedCollection))
    results should containInAnyOrder(Seq("one", "two", "three", "four", "five"))
  }
}

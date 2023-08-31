package org.mkuthan.streamprocessing.shared.scio

import com.spotify.scio.testing.SCollectionMatchers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.TestScioContext

class SContextSyntaxTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionMatchers
    with ScioContextSyntax {

  behavior of "SContext syntax"

  it should "union in global window" in runWithScioContext { sc =>
    val collection1 = sc.parallelize(Seq("one")).withGlobalWindow()
    val collection2 = sc.parallelize(Seq("two", "three")).windowByDays(1)
    val collection3 = sc.parallelize(Seq[String]()).windowByMonths(1)

    val results = sc.unionInGlobalWindow(collection1, collection2, collection3)
    results should containInAnyOrder(Seq("one", "two", "three"))
  }
}

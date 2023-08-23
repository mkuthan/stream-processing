package org.mkuthan.streamprocessing.shared.common

import com.spotify.scio.testing._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.TestScioContext

class DiagnosticTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionMatchers {

  behavior of "Diagnostic"

  it should "union diagnostics" in runWithScioContext { sc =>
    val diagnostic1 = Seq(Diagnostic("1", "reason 1"), Diagnostic("1", "reason 2"))
    val diagnostic2 = Seq(Diagnostic("2", "reason 1"), Diagnostic("2", "reason 1"))

    val results = Diagnostic
      .unionAll(sc.parallelize(diagnostic1), sc.parallelize(diagnostic2))
      // diagnostic aggregation using semigroup
      .sumByKey
      .values

    results should containInAnyOrder(Seq(
      Diagnostic("1", "reason 1", 1),
      Diagnostic("1", "reason 2", 1),
      Diagnostic("2", "reason 1", 2)
    ))
  }

}

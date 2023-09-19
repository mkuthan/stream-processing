package org.mkuthan.streamprocessing.shared.scio.syntax

import org.joda.time.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio.SumByKey
import org.mkuthan.streamprocessing.test.scio.boundedTestCollectionOf
import org.mkuthan.streamprocessing.test.scio.TestScioContext

object SCollectionSumByKeyOpsTest {
  case class SampleClass(key: String, count: Int = 1)

  implicit val SampleClassSumByKey: SumByKey[SampleClass] = SumByKey.create(
    keyFn = _.key,
    plusFn = (x, y) => x.copy(count = x.count + y.count)
  )
}

class SCollectionSumByKeyOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with SCollectionSyntax {

  import SCollectionSumByKeyOpsTest._

  behavior of "SCollectionSumByKey syntax"

  it should "sum by key in fixed window" in runWithScioContext { sc =>
    val sample1 = SampleClass(key = "k1")
    val sample2 = SampleClass(key = "k2")

    val collection = boundedTestCollectionOf[SampleClass]
      .addElementsAtTime("12:00:00", sample1)
      .addElementsAtTime("12:01:00", sample2)
      .addElementsAtTime("12:02:00", sample1)
      .addElementsAtTime("12:10:00", sample2)
      .build()

    val results = sc.testBounded(collection).sumByKeyInFixedWindow(Duration.standardMinutes(10))

    results should inOnTimePane("12:00:00", "12:10:00") {
      containInAnyOrder(Seq(sample1.copy(count = 2), sample2.copy(count = 1)))
    }

    results should inOnTimePane("12:10:00", "12:20:00") {
      containInAnyOrder(Seq(sample2.copy(count = 1)))
    }
  }
}

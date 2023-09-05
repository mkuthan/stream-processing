package org.mkuthan.streamprocessing.infrastructure.bigquery

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.scio._

class BigQuerySCollectionDeadLetterOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with IntegrationTestFixtures {

  "BigQuery SCollection DeadLetter syntax" should "map unbounded dead letter into diagnostic" in runWithScioContext {
    sc =>
      val id1 = IoIdentifier[SampleClass]("id 1")
      val id2 = IoIdentifier[SampleClass]("id 2")
      val error = "any error"

      val deadLetter1 = BigQueryDeadLetter(id1, SampleObject1, error)
      val deadLetter2 = BigQueryDeadLetter(id2, SampleObject2, error)

      val deadLetters = unboundedTestCollectionOf[BigQueryDeadLetter[SampleClass]]
        .addElementsAtWatermarkTime(deadLetter1, deadLetter2)
        .advanceWatermarkToInfinity()

      val results = sc.testUnbounded(deadLetters).toDiagnostic()

      results should containInAnyOrder(Seq(
        IoDiagnostic(id1.id, error),
        IoDiagnostic(id2.id, error)
      ))
  }

}

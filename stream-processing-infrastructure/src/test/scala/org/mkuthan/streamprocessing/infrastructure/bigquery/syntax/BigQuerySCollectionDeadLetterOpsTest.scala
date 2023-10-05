package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.scio._

class BigQuerySCollectionDeadLetterOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with IntegrationTestFixtures {

  "BigQuery SCollection DeadLetter syntax" should "map unbounded dead letter into IO diagnostic" in runWithScioContext {
    sc =>
      val id = IoIdentifier[SampleClass]("any id")

      val deadLetter1 = BigQueryDeadLetter(SampleObject1, "error 1")
      val deadLetter2 = BigQueryDeadLetter(SampleObject2, "error 2")

      val deadLetters = unboundedTestCollectionOf[BigQueryDeadLetter[SampleClass]]
        .addElementsAtWatermarkTime(deadLetter1, deadLetter2)
        .advanceWatermarkToInfinity()

      val results = sc.testUnbounded(deadLetters).toIoDiagnostic(id)

      results should containInAnyOrder(Seq(
        IoDiagnostic(id.id, "error 1"),
        IoDiagnostic(id.id, "error 2")
      ))
  }

}

package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.scio._

class PubsubSCollectionDeadLetterOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with IntegrationTestFixtures {

  "Pubsub SCollection DeadLetter syntax" should "map unbounded dead letter into IO diagnostic" in runWithScioContext {
    sc =>
      val id = IoIdentifier[SampleClass]("any id")

      val deadLetter1 = PubsubDeadLetter[SampleClass](SampleJson1, SampleMap1, "error 1")
      val deadLetter2 = PubsubDeadLetter[SampleClass](SampleJson1, SampleMap1, "error 2")

      val deadLetters = unboundedTestCollectionOf[PubsubDeadLetter[SampleClass]]
        .addElementsAtWatermarkTime(deadLetter1, deadLetter2)
        .advanceWatermarkToInfinity()

      val results = sc.testUnbounded(deadLetters).toIoDiagnostic(id)

      results should containInAnyOrder(Seq(
        IoDiagnostic(id.id, "error 1"),
        IoDiagnostic(id.id, "error 2")
      ))
  }
}

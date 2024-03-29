package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class PubsubSCollectionDeadLetterOpsTest extends AnyFlatSpec
    with Matchers
    with TestScioContext
    with IntegrationTestFixtures {

  "Pubsub SCollection DeadLetter syntax" should "map unbounded dead letter into diagnostic" in runWithScioContext {
    sc =>
      val id = IoIdentifier[SampleClass]("any id")

      val deadLetter1 = PubsubDeadLetter[SampleClass](SampleJson1, SampleMap1, "error 1")
      val deadLetter2 = PubsubDeadLetter[SampleClass](SampleJson1, SampleMap1, "error 2")

      val deadLetters = unboundedTestCollectionOf[PubsubDeadLetter[SampleClass]]
        .addElementsAtWatermarkTime(deadLetter1, deadLetter2)
        .advanceWatermarkToInfinity()

      val results = sc.testUnbounded(deadLetters).toDiagnostic(id)

      results should containElements(
        Diagnostic(id.id, "error 1"),
        Diagnostic(id.id, "error 2")
      )
  }
}

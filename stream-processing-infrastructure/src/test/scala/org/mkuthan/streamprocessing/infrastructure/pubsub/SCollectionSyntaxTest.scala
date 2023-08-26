package org.mkuthan.streamprocessing.infrastructure.pubsub

import scala.collection.mutable

import com.spotify.scio.testing._

import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.test.gcp.PubsubContext
import org.mkuthan.streamprocessing.test.scio._

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with PubsubContext {

  import IntegrationTestFixtures._

  behavior of "Pubsub SCollection syntax"

  it should "publish JSON" in withScioContext { sc =>
    withTopic { topic =>
      withSubscription(topic) { subscription =>
        sc
          .parallelize[Message[SampleClass]](Seq(
            Message(SampleObject1, SampleMap1),
            Message(SampleObject2, SampleMap2)
          ))
          .publishJsonToPubSub(IoIdentifier[SampleClass]("any-id"), PubsubTopic[SampleClass](topic))

        sc.run().waitUntilDone()

        val results = mutable.ArrayBuffer.empty[(SampleClass, Map[String, String])]
        eventually {
          results ++= pullMessages(subscription)
            .map { case (payload, attributes) =>
              (JsonSerde.readJsonFromBytes[SampleClass](payload).get, attributes)
            }

          results should contain.only(
            (SampleObject1, SampleMap1),
            (SampleObject2, SampleMap2)
          )
        }
      }
    }
  }

  it should "map unbounded dead letter into diagnostic" in withScioContext { sc =>
    val instant = Instant.parse("2014-09-10T12:01:00.000Z")

    val deadLetter1 = PubsubDeadLetter(IoIdentifier[SampleClass]("id 1"), SampleJson1, SampleMap1, "error 1")
    val deadLetter2 = PubsubDeadLetter(IoIdentifier[SampleClass]("id 2"), SampleJson1, SampleMap1, "error 2")

    val deadLetters = testStreamOf[PubsubDeadLetter[SampleClass]]
      .addElementsAtTime(instant.toString, deadLetter1, deadLetter2)
      .advanceWatermarkToInfinity()

    val results = sc.testStream(deadLetters).toDiagnostic()

    results should containInAnyOrder(Seq(
      Diagnostic(instant, "id 1", "error 1"),
      Diagnostic(instant, "id 2", "error 2")
    ))
  }
}

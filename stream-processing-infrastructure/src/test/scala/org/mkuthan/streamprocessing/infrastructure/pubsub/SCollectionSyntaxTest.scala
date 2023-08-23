package org.mkuthan.streamprocessing.infrastructure.pubsub

import scala.collection.mutable

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
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

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

  it should "map dead letter into diagnostic" in withScioContext { sc =>
    val deadLetter = PubsubDeadLetter(IoIdentifier[SampleClass]("id"), SampleJson1, SampleMap1, "error")

    val results = sc.parallelize(Seq(deadLetter)).toDiagnostic()

    results should containSingleValue(Diagnostic("id", "error"))
  }
}

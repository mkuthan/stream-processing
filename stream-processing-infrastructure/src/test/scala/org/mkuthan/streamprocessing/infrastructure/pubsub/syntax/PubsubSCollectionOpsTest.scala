package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import scala.collection.mutable

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubTopic
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.PubsubClient._
import org.mkuthan.streamprocessing.test.gcp.PubsubContext
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class PubsubSCollectionOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with PubsubContext {

  import IntegrationTestFixtures._

  behavior of "Pubsub SCollection syntax"

  it should "publish bounded JSON" in withScioContext { sc =>
    withTopic { topic =>
      withSubscription(topic) { subscription =>
        val message1 = Message(SampleObject1, SampleMap1)
        val message2 = Message(SampleObject2, SampleMap2)

        val input = boundedTestCollectionOf[Message[SampleClass]]
          .addElementsAtMinimumTime(message1, message2)
          .advanceWatermarkToInfinity()

        sc.testBounded(input).publishJsonToPubsub(
          IoIdentifier[SampleClass]("any-id"),
          PubsubTopic[SampleClass](topic)
        )

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

  it should "publish unbounded JSON" in withScioContext { sc =>
    withTopic { topic =>
      withSubscription(topic) { subscription =>
        val message1 = Message(SampleObject1, SampleMap1)
        val message2 = Message(SampleObject2, SampleMap2)

        val input = unboundedTestCollectionOf[Message[SampleClass]]
          .addElementsAtWatermarkTime(message1, message2)
          .advanceWatermarkToInfinity()

        sc.testUnbounded(input).publishJsonToPubsub(
          IoIdentifier[SampleClass]("any-id"),
          PubsubTopic[SampleClass](topic)
        )

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
}

package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import scala.collection.mutable

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.gcp.PubsubContext
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.infrastructure.scio._

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with PubsubContext {

  import IntegrationTestFixtures._

  behavior of "Pubsub SCollection syntax"

  it should "publish JSON" in withScioContext { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        sc
          .parallelize[PubsubMessage[SampleClass]](Seq(
            PubsubMessage(SampleObject1, SampleMap1),
            PubsubMessage(SampleObject2, SampleMap2)
          ))
          .publishJsonToPubSub(topic)

        sc.run().waitUntilDone()

        val results = mutable.ArrayBuffer.empty[(SampleClass, Map[String, String])]
        eventually {
          results ++= pullMessages(subscription.id)
            .map { case (payload, attributes) =>
              (readJsonFromBytes[SampleClass](payload).get, attributes)
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

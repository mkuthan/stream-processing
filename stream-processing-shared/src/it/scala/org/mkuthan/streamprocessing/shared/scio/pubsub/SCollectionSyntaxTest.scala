package org.mkuthan.streamprocessing.shared.scio.pubsub

import scala.collection.mutable

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic
import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.test.gcp.PubsubContext
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with PubsubContext {

  import org.mkuthan.streamprocessing.shared.scio.IntegrationTestFixtures._

  behavior of "Pubsub SCollection syntax"

  it should "publish JSON" in withScioContext { sc =>
    withTopic { topic =>
      withSubscription(topic) { subscription =>
        sc
          .parallelize[PubsubMessage[SampleClass]](Seq(
            PubsubMessage(SampleObject1, SampleMap1),
            PubsubMessage(SampleObject2, SampleMap2)
          ))
          .publishJsonToPubSub(IoIdentifier("any-id"), PubSubTopic[SampleClass](topic))

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

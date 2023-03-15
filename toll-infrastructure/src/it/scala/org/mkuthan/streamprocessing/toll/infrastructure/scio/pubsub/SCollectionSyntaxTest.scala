package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import scala.collection.mutable

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.it.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.it.context.ItScioContext
import org.mkuthan.streamprocessing.shared.it.context.PubSubContext
import org.mkuthan.streamprocessing.shared.it.gcp.PubSubClient._
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.infrastructure.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub.PubSubMessage

class SCollectionSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with ItScioContext
    with PubSubContext {

  import IntegrationTestFixtures._

  behavior of "PubSub SCollection syntax"

  it should "publish JSON messages" in withScioContext { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        sc
          .parallelize[PubSubMessage[SampleClass]](Seq(
            PubSubMessage(SampleObject1, SampleMap1),
            PubSubMessage(SampleObject2, SampleMap2)
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

package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.collection.mutable

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes

class SCollectionPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with PubSubScioContext
    with SCollectionPubSubSyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionPubSubSyntax"

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

package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.joda.time.Instant
import org.mkuthan.streamprocessing.shared.test.common.IntegrationTestPatience
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mkuthan.streamprocessing.shared.test.common.RandomString.randomString
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.infrastructure.scio.PubSubAttribute.DefaultId
import org.mkuthan.streamprocessing.toll.infrastructure.scio.PubSubAttribute.DefaultTimestamp

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
        val attr1 =
          Map(DefaultId.name -> randomString(), DefaultTimestamp.name -> Instant.now().toString, "key" -> "value1")
        val attr2 =
          Map(DefaultId.name -> randomString(), DefaultTimestamp.name -> Instant.now().toString, "key" -> "value2")

        sc
          .parallelize[PubSubMessage[SampleClass]](Seq(
            PubSubMessage(SampleObject1, attr1),
            PubSubMessage(SampleObject2, attr2)
          ))
          .publishJsonToPubSub(topic, Some(DefaultId), Some(DefaultTimestamp))

        sc.run().waitUntilDone()

        eventually {
          val results = pullMessages(subscription.id)
            .map { case (payload, attributes) =>
              (readJsonFromBytes[SampleClass](payload).get, attributes)
            }

          results should contain.only(
            (SampleObject1, attr1),
            (SampleObject2, attr2)
          )
        }
      }
    }
  }
}

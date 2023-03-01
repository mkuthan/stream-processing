package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  it should "publish messages" in withScioContext { sc =>
    withTopic[ComplexClass] { topic =>
      withSubscription[ComplexClass](topic.id) { subscription =>
        val attr1 = Map("key" -> "value1")
        val attr2 = Map("key" -> "value2")

        sc
          .parallelize[PubSubMessage[ComplexClass]](Seq(
            PubSubMessage(complexObject1, attr1),
            PubSubMessage(complexObject2, attr2)
          ))
          .publishToPubSub(topic)

        sc.run().waitUntilDone()

        eventually {
          val results = pullMessages(subscription.id)
            .map { case (payload, attributes) => (readJsonFromBytes[ComplexClass](payload), attributes) }
            .flatMap(_.toOption)

          results should contain.only(
            (complexObject1, attr1),
            (complexObject2, attr2)
          )
        }
      }
    }
  }
}

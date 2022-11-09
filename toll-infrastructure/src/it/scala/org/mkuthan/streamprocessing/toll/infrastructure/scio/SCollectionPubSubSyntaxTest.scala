package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJson

class SCollectionPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with PubSubScioContext
    with SCollectionPubSubSyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionPubSubSyntax"

  it should "publish messages" in withScioContext { sc =>
    withTopic[ComplexClass] { topic =>
      withSubscription[ComplexClass](topic.topic) { subscription =>
        sc
          .parallelize[ComplexClass](Seq(complexObject1, complexObject2))
          .publishToPubSub(topic)

        sc.run().waitUntilDone()

        eventually {
          val results = pullMessages(subscription.subscription)
            .map(readJson[ComplexClass])

          // TODO: check for id/ts attributes
          results should contain.only(complexObject1, complexObject2)
        }
      }
    }
  }
}

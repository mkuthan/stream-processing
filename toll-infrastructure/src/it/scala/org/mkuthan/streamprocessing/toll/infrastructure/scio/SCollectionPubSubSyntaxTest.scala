package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic

class SCollectionPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with Eventually
    with IntegrationPatience
    with IntegrationTestScioContext
    with PubSubClient
    with SCollectionPubSubSyntax {

  import IntegrationTestFixtures._

  val topicName = generateTopicName()
  val subscriptionName = generateSubscriptionName()

  val pubSubTopic = PubSubTopic[ComplexClass](topicName)

  override def beforeAll(): Unit = {
    createTopic(topicName)
    createSubscription(topicName, subscriptionName)
  }

  override def afterAll(): Unit = {
    deleteSubscription(subscriptionName)
    deleteTopic(topicName)
  }

  behavior of "SCollectionPubSubSyntax"

  it should "publish messages" in withScioContext { sc =>
    sc
      .parallelize[ComplexClass](Seq(complexObject1, complexObject2))
      .publishToPubSub(pubSubTopic)

    sc.run().waitUntilDone()

    eventually {
      val results = pullMessages(subscriptionName)
        .map(JsonSerde.read[ComplexClass])

      results should contain.only(complexObject1, complexObject2)
    }
  }
}

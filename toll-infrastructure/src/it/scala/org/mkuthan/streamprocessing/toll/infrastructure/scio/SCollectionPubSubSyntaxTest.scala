package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.util.Using

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import com.google.cloud.ServiceOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

class SCollectionPubSubSyntaxTest extends PipelineSpec
  with BeforeAndAfterAll
  with PubSubClient
  with SCollectionPubSubSyntax {

  private val options = PipelineOptionsFactory.create()

  val projectId = ServiceOptions.getDefaultProjectId

  val topicName = generateTopicName()
  val subscriptionName = generateSubscriptionName()

  val pubSubTopic = PubSubTopic[AnyCaseClass](topicName)

  private val record1 = AnyCaseClass("foo", 1)
  private val record2 = AnyCaseClass("foo", 2)

  override def beforeAll(): Unit = {
    createTopic(topicName)
    createSubscription(topicName, subscriptionName)
  }

  override def afterAll(): Unit = Using.Manager { use =>
    deleteSubscription(subscriptionName)
    deleteTopic(topicName)
  }

  behavior of "SCollectionPubSubSyntax"

  it should "publish message" in {
    val sc = ScioContext(options)

    val stream = sc.parallelize[AnyCaseClass](Seq(record1, record2))

    stream.publishToPubSub(pubSubTopic)

    sc.run().waitUntilDone()

    val results = pullMessages(subscriptionName)
      .map(JsonSerde.read[AnyCaseClass])

    results should contain allOf(record1, record2)
  }
}

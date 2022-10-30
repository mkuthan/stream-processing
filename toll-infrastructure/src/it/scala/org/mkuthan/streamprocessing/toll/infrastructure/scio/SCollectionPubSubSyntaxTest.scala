package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.UUID

import scala.util.Using

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.ServiceOptions
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PushConfig
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

class SCollectionPubSubSyntaxTest extends PipelineSpec
  with BeforeAndAfterAll
  with SCollectionPubSubSyntax {

  private val options = PipelineOptionsFactory.create()

  val projectId = ServiceOptions.getDefaultProjectId

  val topicName = s"projects/$projectId/topics/gcloud-test-pubsub-topic-temp-" + UUID.randomUUID().toString
  val subscriptionName =
    s"projects/$projectId/subscriptions/gcloud-test-pubsub-subscription-temp-" + UUID.randomUUID().toString

  val pubSubTopic = PubSubTopic[AnyCaseClass](topicName)

  private val record1 = AnyCaseClass("foo", 1)
  private val record2 = AnyCaseClass("foo", 2)

  override def beforeAll(): Unit =
    Using.Manager { use =>
      val topicAdmin = use(TopicAdminClient.create())
      topicAdmin.createTopic(topicName)

      val subscriptionAdminClient = use(SubscriptionAdminClient.create())
      val pushConfig = PushConfig.newBuilder.build
      val ackDeadlineSeconds = 10
      subscriptionAdminClient.createSubscription(subscriptionName, topicName, pushConfig, ackDeadlineSeconds)
    }.get

  override def afterAll(): Unit = Using.Manager { use =>
    val topicAdmin = use(TopicAdminClient.create())
    val subscriptionAdminClient = use(SubscriptionAdminClient.create())

    subscriptionAdminClient.deleteSubscription(subscriptionName)
    topicAdmin.deleteTopic(topicName)
  }

  behavior of "SCollectionPubSubSyntax"

  it should "publish message" in {
    val sc = ScioContext(options)

    val stream = sc.parallelize[AnyCaseClass](Seq(record1, record2))

    stream.publishToPubSub(pubSubTopic)

    sc.run().waitUntilDone()

    val subscriberStubSettings = SubscriberStubSettings.newBuilder().build()
    Using(GrpcSubscriberStub.create(subscriberStubSettings)) { subscriberStub =>
      val response =
        subscriberStub
          .pullCallable()
          .call(
            PullRequest.newBuilder()
              // pull more than you send, just in case there are other issues.
              .setMaxMessages(10)
              .setSubscription(subscriptionName)
              .build()
          )

      response.getReceivedMessagesCount should be(2)

      import scala.jdk.CollectionConverters._

      val responses = response.getReceivedMessagesList.asScala.toSeq

      val acks = responses.map(_.getAckId)

      subscriberStub
        .acknowledgeCallable()
        .call(
          AcknowledgeRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAllAckIds(acks.asJava)
            .build()
        )

      val results = responses
        .map(_.getMessage.getData.toStringUtf8)
        .map(JsonSerde.read[AnyCaseClass])

      results should contain allOf(record1, record2)
    }
  }
}

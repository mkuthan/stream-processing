package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.UUID

import scala.util.Using

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.ServiceOptions
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PushConfig

trait PubSubClient {

  private val projectId = ServiceOptions.getDefaultProjectId

  def generateTopicName(): String =
    s"projects/$projectId/topics/gcloud-test-pubsub-topic-temp-" + UUID.randomUUID().toString

  def generateSubscriptionName(): String =
    s"projects/$projectId/subscriptions/gcloud-test-pubsub-subscription-temp-" + UUID.randomUUID().toString

  def createTopic(topicName: String): Unit =
    Using(TopicAdminClient.create()) { topicAdminClient =>
      topicAdminClient.createTopic(topicName)
    }.get // fail fast

  def createSubscription(topicName: String, subscriptionName: String): Unit =
    Using(SubscriptionAdminClient.create()) { subscriptionAdminClient =>
      val pushConfig = PushConfig.newBuilder.build
      val ackDeadlineSeconds = 10
      subscriptionAdminClient.createSubscription(subscriptionName, topicName, pushConfig, ackDeadlineSeconds)
    }.get // fail fast

  def deleteTopic(topicName: String): Unit =
    Using(TopicAdminClient.create()) { topicAdminClient =>
      topicAdminClient.deleteTopic(topicName)
    }.toOption // ignore exception

  def deleteSubscription(subscriptionName: String): Unit =
    Using(SubscriptionAdminClient.create()) { subscriptionAdminClient =>
      subscriptionAdminClient.deleteSubscription(subscriptionName)
    }.toOption // ignore exception

  def pullMessages(subscriptionName: String): Seq[String] = {
    import scala.jdk.CollectionConverters._

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

      responses.map(_.getMessage.getData.toStringUtf8)

    }
  }.get // fail fast
}

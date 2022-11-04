package org.mkuthan.streamprocessing.shared.test.gcp

import scala.jdk.CollectionConverters._

import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.Pubsub
import com.google.api.services.pubsub.PubsubScopes
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging

trait PubSubClient extends GcpClient with LazyLogging {

  private[this] val pubsub = new Pubsub.Builder(
    httpTransport,
    jsonFactory,
    requestInitializer(
      credentials(PubsubScopes.CLOUD_PLATFORM)
    )
  ).setApplicationName(appName).build

  def generateTopicName(): String =
    s"projects/$projectId/topics/test-topic-temp-${randomString()}"

  def generateSubscriptionName(): String =
    s"projects/$projectId/subscriptions/test-subscription-temp-${randomString()}"

  def createTopic(topicName: String): Unit = {
    logger.debug("Create pubsub topic: '{}'", topicName)

    val request = new Topic
    pubsub.projects.topics.create(topicName, request).execute
  }

  def createSubscription(topicName: String, subscriptionName: String): Unit = {
    logger.debug("Create pubsub subscription: '{}'", subscriptionName)

    val request = new Subscription()
      .setTopic(topicName)
      .setAckDeadlineSeconds(10)

    pubsub.projects.subscriptions.create(subscriptionName, request).execute
  }

  def deleteTopic(topicName: String): Unit = {
    logger.debug("Delete pubsub topic: '{}'", topicName)

    pubsub.projects.topics.delete(topicName).execute
  }

  def deleteSubscription(subscriptionName: String): Unit = {
    logger.debug("Delete subscription: '{}'", subscriptionName)

    pubsub.projects.subscriptions.delete(subscriptionName).execute
  }

  def publishMessage(topicName: String, messages: String*): Unit = {
    logger.debug("Publish {} messages to: '{}'", messages.size, topicName)

    val pubsubMessages = messages.map { message =>
      new PubsubMessage()
        .encodeData(ByteString.copyFromUtf8(message).toByteArray)
    }.asJava

    val request = new PublishRequest()
      .setMessages(pubsubMessages)
    pubsub.projects().topics().publish(topicName, request).execute()
  }

  def pullMessages(subscriptionName: String): Seq[String] = {
    logger.debug("Pull messages from: '{}'", subscriptionName)

    val request = new PullRequest()
      .setReturnImmediately(true)
      .setMaxMessages(1000)

    val response = pubsub.projects.subscriptions.pull(subscriptionName, request).execute
    val messages = response.getReceivedMessages.asScala.toSeq

    messages.map { message =>
      ByteString.copyFrom(message.getMessage.decodeData()).toStringUtf8
    }
  }
}

package org.mkuthan.streamprocessing.shared.test.gcp

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.Try

import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.Pubsub
import com.google.api.services.pubsub.PubsubScopes
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.test.RandomString._

trait PubSubClient extends GcpProjectId with LazyLogging {

  import GoogleJsonClientUtils._

  private[this] val pubsub = new Pubsub.Builder(
    httpTransport,
    jsonFactory,
    requestInitializer(
      credentials(PubsubScopes.CLOUD_PLATFORM)
    )
  ).setApplicationName(getClass.getName).build

  def generateTopicName(): String =
    s"projects/$projectId/topics/test-topic-temp-${randomString()}"

  def generateSubscriptionName(): String =
    s"projects/$projectId/subscriptions/test-subscription-temp-${randomString()}"

  def createTopic(topicName: String): Unit = {
    logger.debug("Create pubsub topic: '{}'", topicName)

    val request = new Topic
    pubsub.projects.topics.create(topicName, request).execute
    ()
  }

  def createSubscription(topicName: String, subscriptionName: String): Unit = {
    logger.debug("Create pubsub subscription: '{}'", subscriptionName)

    val request = new Subscription()
      .setTopic(topicName)
      .setAckDeadlineSeconds(10) // 10 seconds is a minimum
      .setRetainAckedMessages(true)

    pubsub.projects.subscriptions.create(subscriptionName, request).execute
    ()
  }

  def deleteTopic(topicName: String): Unit = {
    logger.debug("Delete pubsub topic: '{}'", topicName)

    Try(pubsub.projects.topics.delete(topicName).execute).recover {
      case NonFatal(e) => logger.warn("Couldn't delete topic", e)
    }
    ()
  }

  def deleteSubscription(subscriptionName: String): Unit = {
    logger.debug("Delete subscription: '{}'", subscriptionName)

    Try(pubsub.projects.subscriptions.delete(subscriptionName).execute).recover {
      case NonFatal(e) => logger.warn("Couldn't delete subscription", e)
    }
    ()
  }

  def publishMessage(topicName: String, idAttribute: String, tsAttribute: String, messages: String*): Unit = {
    logger.debug("Publish {} messages to: '{}'", messages.size, topicName)

    val now = Instant.now().toString

    val pubsubMessages = messages.map { message =>
      val attributes = Map(
        idAttribute -> randomString(),
        tsAttribute -> now
      ).asJava

      new PubsubMessage()
        .setAttributes(attributes)
        .encodeData(
          ByteString
            .copyFromUtf8(message)
            .toByteArray
        )
    }

    val request = new PublishRequest()
      .setMessages(pubsubMessages.asJava)

    pubsub.projects().topics().publish(topicName, request).execute
    ()
  }

  def pullMessages(subscriptionName: String, maxMessages: Int = 1000): Seq[String] = {
    logger.debug("Pull messages from: '{}'", subscriptionName)

    val request = new PullRequest()
      .setReturnImmediately(true)
      .setMaxMessages(maxMessages)

    val response = pubsub.projects.subscriptions.pull(subscriptionName, request).execute()

    val receivedMessages = if (response.getReceivedMessages == null)
      Seq.empty[ReceivedMessage]
    else
      response.getReceivedMessages.asScala.toSeq

    receivedMessages.map { receivedMessage =>
      val decodedData = if (receivedMessage.getMessage == null || receivedMessage.getMessage.getData == null)
        Array.empty[Byte]
      else
        receivedMessage.getMessage.decodeData()

      ByteString.copyFrom(decodedData).toStringUtf8
    }

    // TODO: ack or not to ack :)
  }
}

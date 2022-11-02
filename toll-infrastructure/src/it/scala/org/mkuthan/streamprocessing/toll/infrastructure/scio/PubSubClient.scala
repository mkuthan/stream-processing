package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.UUID

import com.google.api.services.pubsub.model.PublishRequest
import com.google.api.services.pubsub.model.PubsubMessage
import com.google.api.services.pubsub.model.PullRequest
import com.google.api.services.pubsub.model.Subscription
import com.google.api.services.pubsub.model.Topic
import com.google.api.services.pubsub.Pubsub
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ServiceOptions
import com.google.protobuf.ByteString
import org.apache.beam.sdk.extensions.gcp.util.Transport

trait PubSubClient {

  private val projectId = ServiceOptions.getDefaultProjectId

  val credentials = GoogleCredentials.getApplicationDefault
  val requestInitializer = new HttpCredentialsAdapter(credentials)

  val pubsub = new Pubsub.Builder(
    Transport.getTransport,
    Transport.getJsonFactory,
    requestInitializer
  ).setApplicationName(getClass.getSimpleName).build

  def generateTopicName(): String =
    s"projects/$projectId/topics/gcloud-test-pubsub-topic-temp-" + UUID.randomUUID().toString

  def generateSubscriptionName(): String =
    s"projects/$projectId/subscriptions/gcloud-test-pubsub-subscription-temp-" + UUID.randomUUID().toString

  def createTopic(topicName: String): Unit = {
    val request = new Topic
    pubsub.projects.topics.create(topicName, request).execute
  }

  def createSubscription(topicName: String, subscriptionName: String): Unit = {
    val ackDeadlineSeconds = 10
    val request = new Subscription()
      .setTopic(topicName)
      .setAckDeadlineSeconds(ackDeadlineSeconds)

    pubsub.projects.subscriptions.create(subscriptionName, request).execute
  }

  def deleteTopic(topicName: String): Unit =
    pubsub.projects.topics.delete(topicName).execute

  def deleteSubscription(subscriptionName: String): Unit =
    pubsub.projects.subscriptions.delete(subscriptionName).execute

  def publishMessage(topicName: String, messages: String*): Unit = {
    import scala.jdk.CollectionConverters._

    val pubsubMessages = messages.map { message =>
      new PubsubMessage()
        .encodeData(ByteString.copyFromUtf8(message).toByteArray)
    }.asJava

    val request = new PublishRequest()
      .setMessages(pubsubMessages)
    pubsub.projects().topics().publish(topicName, request).execute()
  }

  def pullMessages(subscriptionName: String): Seq[String] = {
    import scala.jdk.CollectionConverters._

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

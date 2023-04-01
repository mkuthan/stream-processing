package org.mkuthan.streamprocessing.shared.test.gcp

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic

trait PubsubContext {
  this: Suite =>

  import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._

  def withTopic[T](fn: PubSubTopic[T] => Any): Any = {
    val topicName = generateTopicName()
    try {
      createTopic(topicName)
      fn(PubSubTopic[T](topicName))
    } finally
      deleteTopic(topicName)
  }

  def withSubscription[T](
      topicName: String
  )(fn: PubSubSubscription[T] => Any): Any = {
    val subscriptionName = generateSubscriptionName()
    try {
      createSubscription(topicName, subscriptionName)
      fn(PubSubSubscription[T](subscriptionName))
    } finally
      deleteSubscription(subscriptionName)
  }
}

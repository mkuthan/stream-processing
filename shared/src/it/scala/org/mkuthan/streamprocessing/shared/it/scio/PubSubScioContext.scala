package org.mkuthan.streamprocessing.shared.it.scio

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic

trait PubSubScioContext extends GcpScioContext {
  this: Suite =>

  import org.mkuthan.streamprocessing.shared.it.gcp.PubSubClient._

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

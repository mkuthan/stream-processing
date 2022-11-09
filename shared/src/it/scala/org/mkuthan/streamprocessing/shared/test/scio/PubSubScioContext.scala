package org.mkuthan.streamprocessing.shared.test.scio

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic

trait PubSubScioContext extends GcpScioContext with PubSubClient {
  this: Suite =>

  def withTopic[T](fn: PubSubTopic[T] => Any): Any = {
    val topicName = generateTopicName()
    try {
      createTopic(topicName)
      fn(PubSubTopic[T](topicName))
    } finally
      deleteTopic(topicName)
  }

  def withSubscription[T](
      topicName: String,
      idAttribute: Option[String] = None,
      tsAttribute: Option[String] = None
  )(fn: PubSubSubscription[T] => Any): Any = {
    val subscriptionName = generateSubscriptionName()
    try {
      createSubscription(topicName, subscriptionName)
      fn(PubSubSubscription[T](subscriptionName, idAttribute, tsAttribute))
    } finally
      deleteSubscription(subscriptionName)
  }
}

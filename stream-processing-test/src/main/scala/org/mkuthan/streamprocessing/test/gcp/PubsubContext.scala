package org.mkuthan.streamprocessing.test.gcp

import org.scalatest.Suite

trait PubsubContext {
  this: Suite =>

  import PubsubClient._

  def withTopic(fn: String => Any): Any = {
    val topicName = generateTopicName()
    try {
      createTopic(topicName)
      fn(topicName)
    } finally
      deleteTopic(topicName)
  }

  def withSubscription(
      topicName: String
  )(fn: String => Any): Any = {
    val subscriptionName = generateSubscriptionName()
    try {
      createSubscription(topicName, subscriptionName)
      fn(subscriptionName)
    } finally
      deleteSubscription(subscriptionName)
  }
}

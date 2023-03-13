package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.it.common.InMemorySink
import org.mkuthan.streamprocessing.shared.it.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.it.common.RandomString.randomString
import org.mkuthan.streamprocessing.shared.it.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.it.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub.PubSubAttribute.DefaultId
import org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub.PubSubAttribute.DefaultTimestamp

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with PubSubScioContext {

  import IntegrationTestFixtures._

  behavior of "PubSub ScioContext syntax"

  it should "subscribe JSON messages" in withScioContextInBackground { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        publishMessages(
          topic.id,
          (SampleJson1, SampleMap1),
          (SampleJson2, SampleMap2),
          (InvalidJson, SampleMap3)
        )

        val (messages, dlq) = sc.subscribeJsonFromPubSub(subscription)

        val messagesSink = InMemorySink(messages)
        val dlqSink = InMemorySink(dlq)

        val run = sc.run()

        eventually {
          messagesSink.toSeq should contain.only(
            PubSubMessage(SampleObject1, SampleMap1),
            PubSubMessage(SampleObject2, SampleMap2)
          )

          val error = dlqSink.toElement
          error.payload should be(InvalidJson)
          error.attributes should be(SampleMap3)
          error.error should startWith("Unrecognized token 'invalid'")
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "subscribe JSON messages with id attribute" in withScioContextInBackground { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        val id = randomString()
        val attributes = SampleMap1 + (DefaultId.name -> id)

        publishMessages(
          topic.id,
          (SampleJson1, attributes),
          (SampleJson1, attributes), // duplicate
          (SampleJson1, attributes) // duplicate
        )

        val (messages, _) = sc.subscribeJsonFromPubSub(
          subscription = subscription,
          idAttribute = Some(DefaultId)
        )

        val messagesSink = InMemorySink(messages)

        val run = sc.run()

        eventually {
          messagesSink.toSeq should contain.only(PubSubMessage(SampleObject1, attributes))
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "subscribe JSON messages with timestamp attribute" in withScioContextInBackground { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        val timestamp = Instant.now()
        val attributes = SampleMap1 + (DefaultTimestamp.name -> timestamp.toString)

        publishMessages(topic.id, (SampleJson1, attributes))

        val (messages, _) = sc.subscribeJsonFromPubSub(
          subscription = subscription,
          tsAttribute = Some(DefaultTimestamp)
        )

        val messagesSink = InMemorySink(messages.withTimestamp)

        val run = sc.run()

        eventually {
          val (msg, ts) = messagesSink.toElement
          msg should be(PubSubMessage(SampleObject1, attributes))
          ts should be(timestamp)
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

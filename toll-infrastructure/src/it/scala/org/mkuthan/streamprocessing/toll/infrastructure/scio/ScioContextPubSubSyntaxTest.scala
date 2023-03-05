package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.InMemorySink
import org.mkuthan.streamprocessing.shared.test.common.IntegrationTestPatience
import org.mkuthan.streamprocessing.shared.test.common.RandomString._
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes
import org.mkuthan.streamprocessing.toll.infrastructure.scio.PubSubAttribute.DefaultId
import org.mkuthan.streamprocessing.toll.infrastructure.scio.PubSubAttribute.DefaultTimestamp

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with PubSubScioContext
    with ScioContextPubSubSyntax
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  behavior of "ScioContextPubSubSyntaxTest"

  it should "subscribe JSON messages" in withScioContextInBackground { sc =>
    withTopic[SampleClass] { topic =>
      withSubscription[SampleClass](topic.id) { subscription =>
        val attr1 = Map(DefaultId.name -> randomString(), DefaultTimestamp.name -> Instant.now().toString)
        publishMessage(topic.id, writeJsonAsBytes(SampleObject1), attr1)

        val attr2 = Map(DefaultId.name -> randomString(), DefaultTimestamp.name -> Instant.now().toString)
        publishMessage(topic.id, writeJsonAsBytes(SampleObject2), attr2)

        val attr3 = Map(DefaultId.name -> randomString(), DefaultTimestamp.name -> Instant.now().toString)
        publishMessage(topic.id, InvalidJson, attr3)

        val (messages, dlq) = sc
          .subscribeJsonFromPubSub(
            subscription,
            Some(DefaultId),
            Some(DefaultTimestamp)
          )

        val messagesSink = InMemorySink(messages)
        val dlqSink = InMemorySink(dlq)

        val run = sc.run()

        eventually {
          messagesSink.toSeq should contain.only(
            PubSubMessage(SampleObject1, attr1),
            PubSubMessage(SampleObject2, attr2)
          )

          val error = dlqSink.toElement
          error.payload should be(InvalidJson)
          error.attributes should be(attr3)
          error.error should startWith("Unrecognized token 'invalid'")
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

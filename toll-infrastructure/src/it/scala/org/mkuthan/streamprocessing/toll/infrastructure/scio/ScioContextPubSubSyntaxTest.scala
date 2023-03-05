package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.InMemorySink
import org.mkuthan.streamprocessing.shared.test.common.RandomString._
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with PubSubScioContext
    with ScioContextPubSubSyntax
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  private val idAttribute = "id"
  private val tsAttribute = "ts"

  behavior of "ScioContextPubSubSyntaxTest"

  it should "subscribe to topic" in withScioContextInBackground { sc =>
    withTopic[ComplexClass] { topic =>
      withSubscription[ComplexClass](topic.id) { subscription =>
        val attr1 = Map(idAttribute -> randomString(), tsAttribute -> Instant.now().toString)
        publishMessage(topic.id, writeJsonAsBytes(complexObject1), attr1)

        val attr2 = Map(idAttribute -> randomString(), tsAttribute -> Instant.now().toString)
        publishMessage(topic.id, writeJsonAsBytes(complexObject2), attr2)

        val attr3 = Map(idAttribute -> randomString(), tsAttribute -> Instant.now().toString)
        publishMessage(topic.id, invalidJson, attr3)

        val (messages, deserializationErrors) = sc
          .subscribeJsonFromPubSub(subscription, Some(idAttribute), Some(tsAttribute))

        val messagesSink = InMemorySink(messages)
        val deserializationErrorsSink = InMemorySink(deserializationErrors)

        val run = sc.run()

        eventually {
          messagesSink.toSeq should contain.only(
            PubSubMessage(complexObject1, attr1),
            PubSubMessage(complexObject2, attr2)
          )

          val error = deserializationErrorsSink.toElement
          error.payload should be(invalidJson)
          error.attributes should be(attr3)
          error.error should startWith("Unrecognized token 'invalid'")
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

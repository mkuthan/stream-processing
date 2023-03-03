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

        val (results, dlq) = sc
          .subscribeJsonFromPubSub(subscription)

        val resultsSink = InMemorySink(results)
        val dlqSink = InMemorySink(dlq)

        val run = sc.run()

        eventually {
          resultsSink.toSeq should contain.only(
            PubSubMessage(complexObject1, attr1),
            PubSubMessage(complexObject2, attr2)
          )

          dlqSink.toSeq should be(empty)
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.common.RandomString._
import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient._
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromString
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationTestPatience
    with PubSubScioContext
    with ScioContextPubSubSyntax
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  private val globalWindowOptions = WindowOptions(
    trigger = Repeatedly.forever(AfterFirst.of(
      AfterPane.elementCountAtLeast(2),
      AfterProcessingTime
        .pastFirstElementInPane()
        .plusDelayOf(org.joda.time.Duration.standardMinutes(1))
    )),
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    allowedLateness = Duration.standardHours(1) // ts attribute might be slightly behind watermark
  )

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

        val tmpBucket = new StorageBucket[PubSubMessage[ComplexClass]](sc.options.getTempLocation)

        sc
          .subscribeJsonFromPubSub(subscription)
          .withGlobalWindow(globalWindowOptions)
          .saveToStorageAsJson(tmpBucket)

        val run = sc.run()

        eventually {
          val results = readObjectLines(tmpBucket.name, "GlobalWindow-pane-0-00000-of-00001.json")
            .map(readJsonFromString[PubSubMessage[ComplexClass]])
            .flatMap(_.toOption)

          results should contain.only(
            PubSubMessage(complexObject1, attr1),
            PubSubMessage(complexObject2, attr2)
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

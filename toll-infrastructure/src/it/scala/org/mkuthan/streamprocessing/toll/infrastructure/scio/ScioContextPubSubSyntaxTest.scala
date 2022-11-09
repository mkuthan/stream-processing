package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJson
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJson
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with PubSubScioContext
    with PubSubClient
    with StorageClient
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
      withSubscription[ComplexClass](topic.topic, Some(idAttribute), Some(tsAttribute)) { subscription =>
        publishMessage(
          topic.topic,
          idAttribute,
          tsAttribute,
          writeJson(complexObject1),
          writeJson(complexObject2)
        )

        val tmpBucket = new StorageBucket[ComplexClass](bucket = sc.options.getTempLocation, numShards = 1)

        // TODO: contribute to sc.materialize for windowing write support
        sc
          .subscribeToPubSub(subscription)
          .withGlobalWindow(globalWindowOptions)
          .saveToStorageAsJson(tmpBucket)

        val run = sc.run()

        eventually {
          val results = readObjectLines(tmpBucket.name, "GlobalWindow-pane-0-00000-of-00001.json")
            .map(readJson[ComplexClass])

          // TODO: how to verify id attribute and timestamp attribute?
          results should contain.only(complexObject1, complexObject2)
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

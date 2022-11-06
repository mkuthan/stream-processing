package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
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

  behavior of "ScioContextPubSubSyntaxTest"

  it should "subscribe to topic" in withScioContextInBackground { sc =>
    withTopic[ComplexClass] { topic =>
      withSubscription[ComplexClass](topic.id) { subscription =>
        publishMessage(
          topic.id,
          JsonSerde.write(complexObject1),
          JsonSerde.write(complexObject2)
        )

        val tmpBucket = new StorageBucket[ComplexClass](sc.options.getTempLocation)

        sc
          .subscribeToPubSub(subscription)
          .withGlobalWindow(
            WindowOptions(
              trigger = Repeatedly.forever(AfterFirst.of(
                AfterPane.elementCountAtLeast(2),
                AfterProcessingTime
                  .pastFirstElementInPane()
                  .plusDelayOf(org.joda.time.Duration.standardMinutes(1))
              )),
              accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
            )
          )
          .saveToStorage(tmpBucket)

        val run = sc.run()

        eventually {
          val results = readObjectLines(tmpBucket.name, "GlobalWindow-pane-0-00000-of-00001.json")
            .map(JsonSerde.read[ComplexClass])

          results should contain.only(complexObject1, complexObject2)
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

package org.mkuthan.streamprocessing.infrastructure.storage.syntax

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.storage.NumShards
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.infrastructure.storage.StorageConfiguration
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.test.gcp.StorageContext
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

@Slow
class StorageSCollectionOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with StorageContext {

  private val configuration = StorageConfiguration()
    .withNumShards(NumShards.One) // make tests deterministic

  private val tenMinutes = Duration.standardMinutes(10)

  behavior of "Storage SCollection syntax"

  it should "write unbounded on GCS as single JSON file" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = unboundedTestCollectionOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:02:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testUnbounded(sampleObjects)
        .withFixedWindows(tenMinutes)
        .writeUnboundedToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](bucket),
          configuration
        )

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:00:00.000Z"
      val windowEnd = "2014-09-10T12:10:00.000Z"

      eventually {
        val results = readObjectLines(bucket, fileName(windowStart, windowEnd))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }

  it should "write unbounded on GCS two JSON files if there are two windows" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = unboundedTestCollectionOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:11:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testUnbounded(sampleObjects)
        .withFixedWindows(tenMinutes)
        .writeUnboundedToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](bucket),
          configuration
        )

      sc.run().waitUntilDone()

      val firstWindowStart = "2014-09-10T12:00:00.000Z"
      val firstWindowEnd = "2014-09-10T12:10:00.000Z"
      val secondWindowStart = "2014-09-10T12:10:00.000Z"
      val secondWindowEnd = "2014-09-10T12:20:00.000Z"

      eventually {
        val first = readObjectLines(bucket, fileName(firstWindowStart, firstWindowEnd))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)
        val second = readObjectLines(bucket, fileName(secondWindowStart, secondWindowEnd))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        first should contain only SampleObject1
        second should contain only SampleObject2
      }
    }
  }

  it should "write unbounded on GCS as two JSON files if overflows" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = unboundedTestCollectionOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:02:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      val windowOptions = WindowOptions(
        trigger = Repeatedly.forever(
          AfterFirst.of(
            AfterWatermark.pastEndOfWindow(),
            AfterPane.elementCountAtLeast(1)
          )
        ),
        allowedLateness = Duration.ZERO,
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
        onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
      )

      sc
        .testUnbounded(sampleObjects)
        .withFixedWindows(duration = tenMinutes, options = windowOptions)
        .writeUnboundedToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](bucket),
          configuration
        )

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:00:00.000Z"
      val windowEnd = "2014-09-10T12:10:00.000Z"

      eventually {
        val first = readObjectLines(bucket, fileName(windowStart, windowEnd, pane = Some(0)))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)
        val second = readObjectLines(bucket, fileName(windowStart, windowEnd, pane = Some(1)))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        first should contain only SampleObject1
        second should contain only SampleObject2
      }
    }
  }

  def fileName(
      windowStart: String,
      windowEnd: String,
      shard: Int = 0,
      numShards: Int = 1,
      pane: Option[Int] = None
  ): String = {
    val shardFragment = "%05d-of-%05d".formatted(shard, numShards)
    pane match {
      case Some(paneFragment) => s"$windowStart-$windowEnd-$paneFragment-$shardFragment.json"
      case None               => s"$windowStart-$windowEnd-$shardFragment.json"
    }
  }
}

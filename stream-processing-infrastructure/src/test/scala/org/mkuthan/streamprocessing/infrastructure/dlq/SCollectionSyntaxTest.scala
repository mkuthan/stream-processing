package org.mkuthan.streamprocessing.infrastructure.dlq

import com.spotify.scio.testing._

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.storage.NumShards
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.test.gcp.StorageContext
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with StorageContext {

  private val configuration = DlqConfiguration()
    .withNumShards(NumShards.One) // make tests deterministic

  behavior of "DLQ SCollection syntax"

  it should "write on GCS as single JSON file" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = testStreamOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:02:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testStream(sampleObjects)
        .writeDeadLetterToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](s"gs://$bucket"),
          configuration
        )

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:00:00.000Z"
      val windowEnd = "2014-09-10T12:10:00.000Z"
      val shard = "00000-of-00001"

      eventually {
        val results = readObjectLines(bucket, fileName(windowStart, windowEnd, shard))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }

  it should "write on GCS as two JSON files if max records is reached" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = testStreamOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:02:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testStream(sampleObjects)
        .writeDeadLetterToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](s"gs://$bucket"),
          configuration.withMaxRecords(1)
        )

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:00:00.000Z"
      val windowEnd = "2014-09-10T12:10:00.000Z"
      val shard = "00000-of-00001"

      eventually {
        val first = readObjectLines(bucket, fileName(windowStart, windowEnd, shard, pane = "0"))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)
        val second = readObjectLines(bucket, fileName(windowStart, windowEnd, shard, pane = "1"))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        first should contain.only(SampleObject1)
        second should contain.only(SampleObject2)
      }
    }
  }

  ignore should "write on GCS as two JSON files if there are two windows" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = testStreamOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:11:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testStream(sampleObjects)
        .writeDeadLetterToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](s"gs://$bucket")
        )

      sc.run().waitUntilDone()

      val firstWindowStart = "2014-09-10T12:00:00.000Z"
      val firstWindowEnd = "2014-09-10T12:10:00.000Z"
      val secondWindowStart = "2014-09-10T12:10:00.000Z"
      val secondWindowEnd = "2014-09-10T12:20:00.000Z"
      val shard = "00000-of-00001" // shard magic, the reason why the test is ignored

      eventually {
        val first = readObjectLines(bucket, fileName(firstWindowStart, firstWindowEnd, shard))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)
        val second = readObjectLines(bucket, fileName(secondWindowStart, secondWindowEnd, shard))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        first should contain.only(SampleObject1)
        second should contain.only(SampleObject2)
      }
    }
  }

  def fileName(windowStart: String, windowEnd: String, shard: String): String =
    s"$windowStart-$windowEnd-$shard.json"

  def fileName(windowStart: String, windowEnd: String, shard: String, pane: String): String =
    s"$windowStart-$windowEnd-$pane-$shard.json"
}

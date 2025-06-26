package org.mkuthan.streamprocessing.infrastructure.storage.syntax

import org.joda.time.Duration
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.storage.NumShards
import org.mkuthan.streamprocessing.infrastructure.storage.Prefix
import org.mkuthan.streamprocessing.infrastructure.storage.StorageBucket
import org.mkuthan.streamprocessing.infrastructure.storage.StorageConfiguration
import org.mkuthan.streamprocessing.infrastructure.storage.Suffix
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.test.gcp.StorageContext
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

class StorageSCollectionOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with StorageContext {

  private val anyPrefix = Prefix.Explicit("any_prefix")
  private val anySuffix = Suffix.Explicit(".any_suffix")

  private val configuration = StorageConfiguration()
    .withPrefix(anyPrefix)
    .withSuffix(anySuffix)
    .withNumShards(NumShards.One) // make tests deterministic

  behavior of "Storage SCollection syntax"

  it should "write bounded on GCS as single JSON file" in withScioContext { sc =>
    withBucket { bucket =>
      val input = boundedTestCollectionOf[SampleClass]
        .addElementsAtMinimumTime(SampleObject1)
        .addElementsAtMinimumTime(SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testBounded(input)
        .writeToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](bucket),
          configuration
        )

      sc.run().waitUntilDone()

      eventually {
        val results = readObjectLines(bucket, globalFileName(anyPrefix, anySuffix))
          .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }

  it should "write unbounded on GCS as single JSON file in fixed window" in withScioContext { sc =>
    withBucket { bucket =>
      val sampleObjects = unboundedTestCollectionOf[SampleClass]
        .addElementsAtTime("2014-09-10T12:01:00.000Z", SampleObject1)
        .addElementsAtTime("2014-09-10T12:02:00.000Z", SampleObject2)
        .advanceWatermarkToInfinity()

      sc
        .testUnbounded(sampleObjects)
        .withFixedWindows(Duration.standardMinutes(10))
        .writeToStorageAsJson(
          IoIdentifier[SampleClass]("any-id"),
          StorageBucket[SampleClass](bucket),
          configuration
        )

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:00:00.000Z"
      val windowEnd = "2014-09-10T12:10:00.000Z"

      eventually {
        val results =
          readObjectLines(bucket, windowedFileName(anyPrefix, anySuffix, windowStart, windowEnd))
            .map(JsonSerde.readJsonFromString[SampleClass](_).get)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }

  def globalFileName(
      prefix: Prefix.Explicit,
      suffix: Suffix.Explicit,
      shard: Int = 0,
      numShards: Int = 1
  ): String =
    "%s-%05d-of-%05d%s".formatted(prefix.value, shard, numShards, suffix.value)

  def windowedFileName(
      prefix: Prefix.Explicit,
      suffix: Suffix.Explicit,
      windowStart: String,
      windowEnd: String,
      shard: Int = 0,
      numShards: Int = 1
  ): String =
    "%s-%s-%s-%05d-of-%05d%s".formatted(prefix.value, windowStart, windowEnd, shard, numShards, suffix.value)

}

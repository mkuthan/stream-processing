package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

import org.joda.time.Duration
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.shared.test.gcp.StorageContext
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.infrastructure.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.infrastructure.scio.IntegrationTestFixtures.SampleClass

final class SCollectionSyntaxTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with StorageContext {

  behavior of "Storage SCollection syntax"

  it should "save file on GCS in global window" in withScioContext { sc =>
    withBucket[SampleClass] { bucket =>
      sc
        .parallelize[SampleClass](Seq(SampleObject1, SampleObject2))
        .saveToStorageAsJson(IoIdentifier("any-id"), bucket)

      sc.run().waitUntilDone()

      eventually {
        val results =
          readObjectLines(bucket.name, "GlobalWindow-pane-0-last-00000-of-00001.json")
            .map(JsonSerde.readJsonFromString[SampleClass])
            .flatMap(_.toOption)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }

  it should "save file on GCS in fixed window" in withScioContext { sc =>
    withBucket[SampleClass] { bucket =>
      sc
        .parallelizeTimestamped[SampleClass](
          Seq(
            (SampleObject1, SampleObject1.instantField),
            (SampleObject2, SampleObject2.instantField)
          )
        )
        .withFixedWindows(Duration.standardSeconds(10))
        .saveToStorageAsJson(IoIdentifier("any-id"), bucket)

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:03:00.000Z"
      val windowEnd = "2014-09-10T12:03:10.000Z"

      eventually {
        val results =
          readObjectLines(bucket.name, s"$windowStart-$windowEnd-pane-0-last-00000-of-00001.json")
            .map(JsonSerde.readJsonFromString[SampleClass])
            .flatMap(_.toOption)

        results should contain.only(SampleObject1, SampleObject2)
      }
    }
  }
}
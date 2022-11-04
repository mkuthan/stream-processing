package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.joda.time.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.scio.StorageScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

final class SCollectionStorageSyntaxTest extends AnyFlatSpec
    with Matchers
    with StorageScioContext
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  behavior of "SCollectionStorageSyntax"

  it should "save file on GCS in global window" in withScioContext { sc =>
    withBucket[ComplexClass] { bucket =>
      sc
        .parallelize[ComplexClass](Seq(complexObject1, complexObject2))
        .saveToStorage(bucket)

      sc.run().waitUntilDone()

      val results =
        readObjectLines(bucket.name, "GlobalWindow-pane-0-last-00000-of-00001.json")
          .map(JsonSerde.read[ComplexClass])

      results should contain.only(complexObject1, complexObject2)
    }
  }

  it should "save file on GCS in fixed window" in withScioContext { sc =>
    withBucket[ComplexClass] { bucket =>
      sc
        .parallelizeTimestamped[ComplexClass](
          Seq(
            (complexObject1, complexObject1.instantField),
            (complexObject2, complexObject2.instantField)
          )
        )
        .withFixedWindows(Duration.standardSeconds(10))
        .saveToStorage(bucket)

      sc.run().waitUntilDone()

      val windowStart = "2014-09-10T12:03:00.000Z"
      val windowEnd = "2014-09-10T12:03:10.000Z"

      val results =
        readObjectLines(bucket.name, s"$windowStart-$windowEnd-pane-0-last-00000-of-00001.json")
          .map(JsonSerde.read[ComplexClass])

      results should contain.only(complexObject1, complexObject2)
    }
  }
}

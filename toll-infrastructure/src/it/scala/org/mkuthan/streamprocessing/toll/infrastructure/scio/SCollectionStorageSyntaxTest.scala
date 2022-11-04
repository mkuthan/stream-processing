package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.joda.time.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.shared.test.scio.IntegrationTestScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageLocation

final class SCollectionStorageSyntaxTest extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with IntegrationTestScioContext
    with StorageClient
    with SCollectionStorageSyntax {

  import IntegrationTestFixtures._

  private val bucketName = generateBucketName()
  private val location = StorageLocation[ComplexClass](s"gs://$bucketName")

  override def beforeAll(): Unit =
    createBucket(bucketName)

  override def afterAll(): Unit =
    deleteBucket(bucketName)

  behavior of "SCollectionStorageSyntax"

  it should "save file on GCS in global window" in withScioContext { sc =>
    sc
      .parallelize[ComplexClass](Seq(complexObject1, complexObject2))
      .saveToStorage(location)

    sc.run().waitUntilDone()

    val results =
      readObjectLines(bucketName, "GlobalWindow-pane-0-last-00000-of-00001.json")
        .map(JsonSerde.read[ComplexClass])

    results should contain.only(complexObject1, complexObject2)
  }

  it should "save file on GCS in fixed window" in withScioContext { sc =>
    sc
      .parallelizeTimestamped[ComplexClass](
        Seq(
          (complexObject1, complexObject1.instantField),
          (complexObject2, complexObject2.instantField)
        )
      )
      .withFixedWindows(Duration.standardSeconds(10))
      .saveToStorage(location)

    sc.run().waitUntilDone()

    val windowStart = "2014-09-10T12:03:00.000Z"
    val windowEnd = "2014-09-10T12:03:10.000Z"

    val results =
      readObjectLines(bucketName, s"$windowStart-$windowEnd-pane-0-last-00000-of-00001.json")
        .map(JsonSerde.read[ComplexClass])

    results should contain.only(complexObject1, complexObject2)
  }
}

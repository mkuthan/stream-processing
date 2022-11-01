package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

final class SCollectionStorageSyntaxTest extends PipelineSpec
    with BeforeAndAfterAll
    with StorageClient
    with SCollectionStorageSyntax {

  private val options = PipelineOptionsFactory.create()
  private val bucketName = generateBucketName()

  val location = StorageLocation[AnyCaseClass](s"gs://$bucketName")

  private val record1 = AnyCaseClass("foo", 1)
  private val record2 = AnyCaseClass("foo", 2)

  override def beforeAll(): Unit =
    createBucket(bucketName)

  override def afterAll(): Unit =
    deleteBucket(bucketName)

  behavior of "SCollectionStorageSyntax"

  it should "save file on GCS in global window" in {
    val sc = ScioContext(options)

    val stream = sc.parallelize[AnyCaseClass](Seq(record1, record2))

    stream.saveToStorage(location)

    sc.run().waitUntilDone()

    val results =
      readBlobLines(bucketName, "GlobalWindow-pane-0-last-00000-of-00001.json")
        .map(JsonSerde.read[AnyCaseClass])

    results should contain allOf (record1, record2)
  }

  it should "save file on GCS in fixed window" in {
    val sc = ScioContext(options)

    val stream = sc.parallelizeTimestamped[AnyCaseClass](
      Seq(
        (record1, Instant.parse("2014-09-10T12:03:01Z")),
        (record2, Instant.parse("2014-09-10T12:03:02Z"))
      )
    )

    stream
      .withFixedWindows(Duration.standardSeconds(10))
      .saveToStorage(location)

    sc.run().waitUntilDone()

    val windowStart = "2014-09-10T12:03:00.000Z"
    val windowEnd = "2014-09-10T12:03:10.000Z"

    val results =
      readBlobLines(bucketName, s"$windowStart-$windowEnd-pane-0-last-00000-of-00001.json")
        .map(JsonSerde.read[AnyCaseClass])

    results should contain allOf (record1, record2)
  }
}

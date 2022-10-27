package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.ScioContext

import com.google.cloud.storage.testing.RemoteStorageHelper
import com.google.cloud.storage.BucketInfo
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll

final class SCollectionStorageSyntaxTest extends PipelineSpec
    with BeforeAndAfterAll
    with SCollectionStorageSyntax {

  private val options = PipelineOptionsFactory.create()

  private val helper = RemoteStorageHelper.create
  private val storage = helper.getOptions.getService
  private val bucket = RemoteStorageHelper.generateBucketName

  override def beforeAll(): Unit =
    storage.create(BucketInfo.of(bucket))

  override def afterAll() {
    RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS)
  }

  "foo" should "bar" in {
    val sc = ScioContext(options)

    val location = StorageLocation[AnyCaseClass](s"gs://$bucket")
    sc.parallelize[AnyCaseClass](Seq(AnyCaseClass("foo", 1), AnyCaseClass("foo", 2))).saveToStorage(location)

    sc.run().waitUntilDone()

    val blobs = storage.list(bucket).iterateAll().asScala
    println(blobs)

    // TODO assertions
  }
}

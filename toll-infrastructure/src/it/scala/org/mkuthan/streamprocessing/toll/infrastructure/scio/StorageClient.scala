package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.io.BufferedReader
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.immutable.Iterable
import scala.jdk.CollectionConverters._
import scala.util.Using

import com.google.cloud.storage.testing.RemoteStorageHelper
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BucketInfo

trait StorageClient {

  private val helper = RemoteStorageHelper.create
  private val storage = helper.getOptions.getService

  def generateBucketName(): String =
    RemoteStorageHelper.generateBucketName

  def createBucket(bucketName: String): Unit =
    storage.create(BucketInfo.of(bucketName))

  def deleteBucket(bucketName: String): Unit =
    RemoteStorageHelper.forceDelete(storage, bucketName, 5, TimeUnit.SECONDS)

  def readBlobLines(bucketName: String, blobName: String): Iterable[String] =
    Using(new BufferedReader(
      Channels.newReader(
        storage.reader(BlobId.of(bucketName, blobName)),
        StandardCharsets.UTF_8
      )
    ))(_.lines.iterator().asScala.toSeq).get // fail fast
}

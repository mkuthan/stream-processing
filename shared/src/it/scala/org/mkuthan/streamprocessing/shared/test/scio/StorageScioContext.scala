package org.mkuthan.streamprocessing.shared.test.scio

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

trait StorageScioContext extends GcpScioContext with StorageClient {
  this: Suite =>

  def withBucket[T](fn: StorageBucket[T] => Any): Unit = {
    val bucketName = generateBucketName()
    try {
      createBucket(bucketName)
      fn(StorageBucket[T](s"gs://$bucketName"))
    } finally
      deleteBucket(bucketName)
  }
}

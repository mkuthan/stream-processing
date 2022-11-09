package org.mkuthan.streamprocessing.shared.test.scio

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

trait StorageScioContext extends GcpScioContext with StorageClient {
  this: Suite =>

  // TODO: parametrize numShards
  def withBucket[T](fn: StorageBucket[T] => Any): Any = {
    val bucketName = generateBucketName()
    try {
      createBucket(bucketName)
      fn(StorageBucket[T](bucket = s"gs://$bucketName", numShards = 1))
    } finally
      deleteBucket(bucketName)
  }
}

package org.mkuthan.streamprocessing.shared.test.scio

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

trait StorageScioContext extends GcpScioContext {
  this: Suite =>

  def withBucket[T](fn: StorageBucket[T] => Any): Any = {
    val bucketName = generateBucketName()
    try {
      createBucket(bucketName)
      fn(StorageBucket[T](id = s"gs://$bucketName"))
    } finally
      deleteBucket(bucketName)
  }
}

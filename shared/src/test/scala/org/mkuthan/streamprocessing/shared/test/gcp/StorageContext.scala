package org.mkuthan.streamprocessing.shared.test.gcp

import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.configuration.StorageBucket

trait StorageContext {
  this: Suite =>

  import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._

  def withBucket[T](fn: StorageBucket[T] => Any): Any = {
    val bucketName = generateBucketName()
    try {
      createBucket(bucketName)
      fn(StorageBucket[T](id = s"gs://$bucketName"))
    } finally
      deleteBucket(bucketName)
  }
}

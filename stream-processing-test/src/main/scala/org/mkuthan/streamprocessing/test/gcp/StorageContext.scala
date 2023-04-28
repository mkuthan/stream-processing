package org.mkuthan.streamprocessing.test.gcp

import org.scalatest.Suite

trait StorageContext {
  this: Suite =>

  import StorageClient._

  def withBucket(fn: String => Any): Any = {
    val bucketName = generateBucketName()
    try {
      createBucket(bucketName)
      fn(bucketName)
    } finally
      deleteBucket(bucketName)
  }
}

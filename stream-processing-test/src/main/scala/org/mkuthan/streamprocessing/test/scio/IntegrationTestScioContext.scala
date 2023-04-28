package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.TestPipelineOptions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.mkuthan.streamprocessing.test.gcp.GcpProjectId
import org.mkuthan.streamprocessing.test.gcp.StorageClient

trait IntegrationTestScioContext extends BeforeAndAfterAll
    with SCollectionMatchers with TimestampedMatchers
    with GcpProjectId {
  this: Suite =>

  private val tmpBucketName = StorageClient.generateBucketName()

  override def beforeAll(): Unit =
    StorageClient.createBucket(tmpBucketName)

  override def afterAll(): Unit =
    StorageClient.deleteBucket(tmpBucketName)

  def withScioContext(fn: ScioContext => Any): Any = {
    val options = PipelineOptionsFactory.fromArgs(
      s"--appName=${getClass.getName}",
      s"--project=$projectId",
      s"--tempLocation=gs://$tmpBucketName/"
    ).create()

    val sc = ScioContext(options)
    fn(sc)
  }

  def options(implicit sc: ScioContext): TestPipelineOptions = sc.optionsAs[TestPipelineOptions]
}

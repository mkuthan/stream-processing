package org.mkuthan.streamprocessing.shared.test.scio

import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.TestPipelineOptions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.GcpProjectId

trait ItScioContext extends BeforeAndAfterAll with GcpProjectId {
  this: Suite =>

  import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._

  private val tmpBucketName = generateBucketName()

  override def beforeAll(): Unit =
    createBucket(tmpBucketName)

  override def afterAll(): Unit =
    deleteBucket(tmpBucketName)

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

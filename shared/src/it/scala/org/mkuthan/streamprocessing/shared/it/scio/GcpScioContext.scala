package org.mkuthan.streamprocessing.shared.it.scio

import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.TestPipelineOptions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.it.gcp.GcpProjectId

trait GcpScioContext extends BeforeAndAfterAll with GcpProjectId {
  this: Suite =>

  import org.mkuthan.streamprocessing.shared.it.gcp.StorageClient._

  private val tmpBucketName = generateBucketName()

  override def beforeAll(): Unit =
    createBucket(tmpBucketName)

  override def afterAll(): Unit =
    deleteBucket(tmpBucketName)

  def withScioContext[T](fn: ScioContext => Any): Any = {
    val sc = ScioContext(pipelineOptions)
    fn(sc)
  }

  def withScioContextInBackground[T](fn: ScioContext => Any): Any = {
    val options = pipelineOptions
    options.setBlockOnRun(false)

    val sc = ScioContext(options)
    fn(sc)
  }

  private def pipelineOptions: TestPipelineOptions =
    PipelineOptionsFactory.fromArgs(
      s"--appName=${getClass.getName}",
      s"--project=$projectId",
      s"--tempLocation=gs://$tmpBucketName/"
    ).as(classOf[TestPipelineOptions])

}

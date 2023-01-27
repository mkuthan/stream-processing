package org.mkuthan.streamprocessing.shared.test.scio

import com.spotify.scio.ScioContext

import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.mkuthan.streamprocessing.shared.test.gcp.GcpProjectId
import org.mkuthan.streamprocessing.shared.test.gcp.StorageClient._

trait GcpScioContext extends BeforeAndAfterAll with GcpProjectId {
  this: Suite =>

  private val tmpBucketName = generateBucketName()

  override def beforeAll(): Unit =
    createBucket(tmpBucketName)

  override def afterAll(): Unit =
    deleteBucket(tmpBucketName)

  def withScioContext[T](fn: ScioContext => Any): Any = {
    val sc = ScioContext(
      PipelineOptionsFactory.fromArgs(
        s"--appName=${getClass.getName}",
        s"--project=$projectId",
        s"--tempLocation=gs://$tmpBucketName/"
      ).create()
    )
    fn(sc)
  }

  def withScioContextInBackground[T](fn: ScioContext => Any): Any = {
    val sc = ScioContext(
      PipelineOptionsFactory.fromArgs(
        s"--appName=${getClass.getName}",
        s"--project=$projectId",
        s"--tempLocation=gs://$tmpBucketName",
        "--blockOnRun=false"
      ).create()
    )
    fn(sc)
  }

}

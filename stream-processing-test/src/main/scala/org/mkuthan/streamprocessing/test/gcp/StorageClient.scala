package org.mkuthan.streamprocessing.test.gcp

import java.io.BufferedReader
import java.io.InputStreamReader

import scala.collection.immutable.Iterable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.Using

import com.google.api.client.googleapis.batch.json.JsonBatchCallback
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.api.client.http.HttpHeaders
import com.google.api.services.storage.model.Bucket
import com.google.api.services.storage.Storage
import com.google.api.services.storage.StorageScopes
import com.typesafe.scalalogging.LazyLogging

import org.mkuthan.streamprocessing.test.common.RandomString

object StorageClient extends GcpProjectId with LazyLogging {

  import GoogleClientUtils._

  private val storage = new Storage.Builder(
    httpTransport,
    jsonFactory,
    requestInitializer(
      credentials(StorageScopes.CLOUD_PLATFORM)
    )
  ).setApplicationName(getClass.getName).build

  def generateBucketName(): String =
    s"test-bucket-temp-${RandomString.randomString()}"

  def createBucket(bucketName: String): Unit = {
    logger.debug("Create cloud storage bucket: '{}'", bucketName)

    val request = new Bucket()
      .setName(bucketName)
      .setLocation("eu")

    val _ = storage.buckets().insert(projectId, request).execute
  }

  def deleteBucket(bucketName: String): Unit = {
    logger.debug("Delete cloud storage bucket: '{}'", bucketName)

    val objects = storage.objects().list(bucketName).execute
    if (objects.getItems != null) {
      val items = objects.getItems.asScala

      val batch = storage.batch()
      val callback = new JsonBatchCallback[Void]() {
        override def onFailure(e: GoogleJsonError, responseHeaders: HttpHeaders): Unit =
          logger.error("Couldn't delete object {}", e.getMessage)

        override def onSuccess(obj: Void, responseHeaders: HttpHeaders): Unit =
          ()
      }

      items.foreach { item =>
        storage.objects().delete(bucketName, item.getName).queue(batch, callback)
      }
      batch.execute()
    }

    try {
      val _ = storage.buckets().delete(bucketName).execute()
    } catch {
      case NonFatal(e) => logger.warn("Couldn't delete bucket", e)
    }
  }

  def readObjectLines(bucketName: String, objectName: String): Iterable[String] = {
    logger.debug("Read lines from: 'gs://{}/{}'", bucketName, objectName)

    val lines = Using.Manager { use =>
      val is = use(storage.objects().get(bucketName, objectName).executeMediaAsInputStream())
      val reader = use(new BufferedReader(new BufferedReader(new InputStreamReader(is))))

      reader.lines().iterator().asScala.toSeq
    }

    lines.getOrElse(Seq.empty)
  }
}

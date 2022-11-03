package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.collection.immutable.Iterable
import scala.jdk.CollectionConverters._

import com.google.api.services.storage.model.Bucket
import com.google.api.services.storage.Storage
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ServiceOptions
import org.apache.beam.sdk.extensions.gcp.util.Transport

trait StorageClient {

  private val projectId = ServiceOptions.getDefaultProjectId

  val credentials = GoogleCredentials.getApplicationDefault
  val requestInitializer = new HttpCredentialsAdapter(credentials)

  val storage = new Storage.Builder(
    Transport.getTransport,
    Transport.getJsonFactory,
    requestInitializer
  ).setApplicationName(getClass.getSimpleName).build

  def generateBucketName(): String =
    "gcloud-test-bucket-temp-" + UUID.randomUUID.toString

  def createBucket(bucketName: String): Unit = {
    val request = new Bucket()
      .setName(bucketName)
      .setLocation("eu")
    storage.buckets().insert(projectId, request).execute
  }

  def deleteBucket(bucketName: String): Unit = {
    val objects = storage.objects().list(bucketName).execute
    val items = objects.getItems.asScala

    // TODO: batch
    items.foreach { item =>
      storage.objects().delete(bucketName, item.getName).execute
    }

    storage.buckets().delete(bucketName)
  }

  def readObjectLines(bucketName: String, objectName: String): Iterable[String] = {
    val out = new ByteArrayOutputStream()
    storage.objects().get(bucketName, objectName).executeMediaAndDownloadTo(out)

    // TODO: close reader
    val reader = new BufferedReader(new StringReader(new String(out.toByteArray, StandardCharsets.UTF_8)))
    reader.lines().iterator().asScala.toSeq
  }
}

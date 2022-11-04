package org.mkuthan.streamprocessing.shared.test.gcp

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.google.api.services.bigquery.model.Table
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableSchema
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest
import com.google.cloud.bigquery.storage.v1.ReadSession
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory

trait BigQueryClient extends GcpClient with LazyLogging {

  private[this] val options = PipelineOptionsFactory.create().as(classOf[BigQueryOptions])
  private[this] val datasetService = BigQueryServicesFactory.getDatasetService(options)
  private[this] val storageClient = BigQueryServicesFactory.getStorageClient(options)

  def generateDatasetName(): String =
    s"test_dataset_temp_${randomStringUnderscored()}"

  def generateTableName(): String =
    s"test_table_temp_${randomStringUnderscored()}"

  def createDataset(datasetName: String): Unit = {
    logger.debug("Create bigquery dataset: '{}'", datasetName)

    val location = "eu"
    val description = null
    val defaultTableExpirationMs = 3600 * 1000

    datasetService.createDataset(projectId, datasetName, location, description, defaultTableExpirationMs)
  }

  def createTable(datasetName: String, tableName: String, schema: TableSchema): Unit = {
    logger.debug("Create bigquery table: '{}.{}'", datasetName, tableName)

    val tableReference = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datasetName)
      .setTableId(tableName)

    val table = new Table()
      .setTableReference(tableReference)
      .setSchema(schema)

    datasetService.createTable(table)
  }

  def deleteDataset(datasetName: String): Unit = {
    logger.debug("Delete bigquery dataset: '{}'", datasetName)

    datasetService.deleteDataset(projectId, datasetName)
  }

  def deleteTable(datasetName: String, tableName: String): Unit = {
    logger.debug("Delete bigquery table: '{}.{}'", datasetName, tableName)

    val tableReference = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datasetName)
      .setTableId(tableName)

    datasetService.deleteTable(tableReference)
  }

  def read(datasetName: String, tableName: String): Iterable[GenericRecord] = {
    logger.debug("Read from bigquery table {}.{}", datasetName, tableName)

    val parent = s"projects/$projectId"
    val table = s"projects/$projectId/datasets/$datasetName/tables/$tableName"

    val request = CreateReadSessionRequest
      .newBuilder
      .setParent(parent)
      .setMaxStreamCount(1)
      .setReadSession(ReadSession.newBuilder
        .setTable(table)
        .setDataFormat(DataFormat.AVRO))
      .build

    val session = storageClient.createReadSession(request)

    val readRowsRequest = ReadRowsRequest
      .newBuilder()
      .setReadStream(session.getStreams(0).getName) // TODO: check streams
      .build()

    val rows = storageClient.readRows(readRowsRequest).asScala

    val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)
    val reader = new GenericDatumReader[GenericRecord](schema)
    var decoder: BinaryDecoder = null

    rows.flatMap { row =>
      decoder =
        DecoderFactory.get()
          .binaryDecoder(row.getAvroRows.toByteArray(), decoder);

      val results = ArrayBuffer.empty[GenericRecord]
      while (!decoder.isEnd)
        results += reader.read(null, decoder)

      results
    }
  }

}

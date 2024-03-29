package org.mkuthan.streamprocessing.test.gcp

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesFactory
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow

import com.google.api.services.bigquery.model.Table
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.api.services.bigquery.model.TimePartitioning
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

import org.mkuthan.streamprocessing.test.common.RandomString._

object BigQueryClient extends GcpProjectId with LazyLogging {

  private val options = PipelineOptionsFactory.create().as(classOf[BigQueryOptions])
  private val datasetService = BigQueryServicesFactory.getDatasetService(options)
  private val storageClient = BigQueryServicesFactory.getStorageClient(options)

  def generateDatasetName(): String =
    s"test_dataset_temp_${randomStringUnderscored()}"

  def generateTableName(): String =
    s"test_table_temp_${randomStringUnderscored()}"

  def createDataset(datasetName: String): Unit = {
    logger.debug("Create bigquery dataset: '{}'", datasetName)

    val location = "eu"
    val description = "dataset for automated tests"
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

  def createPartitionedTable(
      datasetName: String,
      tableName: String,
      partitionType: String,
      schema: TableSchema
  ): Unit = {
    logger.debug(
      "Create partitioned by '{}' bigquery table: '{}.{}'",
      partitionType,
      datasetName,
      tableName
    )

    val tableReference = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datasetName)
      .setTableId(tableName)

    val timePartitioning = new TimePartitioning()
      .setType(partitionType)

    val table = new Table()
      .setTableReference(tableReference)
      .setTimePartitioning(timePartitioning)
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

  def readTable(datasetName: String, tableName: String): Iterable[GenericRecord] = {
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
          .binaryDecoder(
            row
              .getAvroRows
              .getSerializedBinaryRows
              .toByteArray,
            decoder
          )

      val results = ArrayBuffer.empty[GenericRecord]
      while (!decoder.isEnd)
        results += reader.read(null, decoder)

      results
    }
  }

  def writeTable(datasetName: String, tableName: String, records: TableRow*): Unit = {
    logger.debug("Write {} records to BigQuery table {}.{}", records.size, datasetName, tableName)
    val tableReference = new TableReference()
      .setProjectId(projectId)
      .setDatasetId(datasetName)
      .setTableId(tableName)

    val rows = records
      .map(record =>
        FailsafeValueInSingleWindow.of(
          record,
          BoundedWindow.TIMESTAMP_MIN_VALUE,
          GlobalWindow.INSTANCE,
          PaneInfo.NO_FIRING,
          record
        )
      )
      .asJava

    // TODO: use Storage Write API
    val _ = datasetService.insertAll[AnyRef](
      tableReference,
      rows,
      null,
      InsertRetryPolicy.neverRetry(),
      null,
      null,
      false,
      false,
      false,
      null
    )
  }

}

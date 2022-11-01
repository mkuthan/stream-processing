package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.UUID

import scala.jdk.CollectionConverters._

import com.google.cloud.bigquery._
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.bigquery.QueryJobConfiguration

trait BigQueryClient {

  private val bigQueryHelper = RemoteBigQueryHelper.create
  private val bigQuery = bigQueryHelper.getOptions.getService

  def generateDatasetName(): String =
    RemoteBigQueryHelper.generateDatasetName()

  def generateTableName(): String =
    "table_" + UUID.randomUUID.toString.replace('-', '_')

  def createDataset(datasetName: String): Unit =
    bigQuery.create(DatasetInfo.of(datasetName))

  def createTable(datasetName: String, tableName: String, schema: Schema): Unit =
    bigQuery.create(
      TableInfo.of(
        TableId.of(datasetName, tableName),
        StandardTableDefinition.of(schema)
      )
    )

  def deleteDataset(datasetName: String): Unit =
    RemoteBigQueryHelper.forceDelete(bigQuery, datasetName)

  def query(query: String) =
    bigQuery.query(
      QueryJobConfiguration
        .newBuilder(query)
        .build
    ).iterateAll().iterator().asScala.toSeq

}

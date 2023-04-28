package org.mkuthan.streamprocessing.test.gcp

import com.google.api.services.bigquery.model.TableSchema
import org.scalatest.Suite

trait BigQueryContext {
  this: Suite =>

  import BigQueryClient._

  def withDataset(fn: String => Any): Any = {
    val datasetName = generateDatasetName()
    try {
      createDataset(datasetName)
      fn(datasetName)
    } finally
      deleteDataset(datasetName)
  }

  def withTable(datasetName: String, schema: TableSchema)(fn: String => Any): Any = {
    val tableName = generateTableName()
    createTable(datasetName, tableName, schema)
    try
      fn(tableName)
    finally
      deleteTable(datasetName, tableName)
  }
}

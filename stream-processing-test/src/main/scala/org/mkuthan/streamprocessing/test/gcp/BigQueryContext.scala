package org.mkuthan.streamprocessing.test.gcp

import scala.reflect.runtime.universe.TypeTag

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation

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

  def withTable[T <: HasAnnotation: TypeTag](datasetName: String)(fn: String => Any): Any = {
    val tableName = generateTableName()
    createTable(datasetName, tableName, BigQueryType[T].schema)
    try
      fn(tableName)
    finally
      deleteTable(datasetName, tableName)
  }
}

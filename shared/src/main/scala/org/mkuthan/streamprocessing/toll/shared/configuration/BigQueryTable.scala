package org.mkuthan.streamprocessing.toll.shared.configuration

import com.spotify.scio.bigquery.Table

final case class BigQueryTable[T](datasetId: String, tableId: String) {
  val spec = Table.Spec(s"$datasetId.$tableId")
}

object BigQueryTable {
  def fromString[T](definition: String): BigQueryTable[T] =
    definition.split('.') match {
      case Array(datasetName, tableName) =>
        new BigQueryTable[T](datasetName, tableName)
      case _ => throw new IllegalArgumentException(
          s"Invalid BigQuery table definition $definition, expected format 'dataset.table'"
        )
    }
}

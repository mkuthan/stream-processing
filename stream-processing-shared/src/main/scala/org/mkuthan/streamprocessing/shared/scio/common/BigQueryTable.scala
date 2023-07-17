package org.mkuthan.streamprocessing.shared.scio.common

case class BigQueryTable[T](id: String) {

  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers

  private lazy val spec = BigQueryHelpers.parseTableSpec(id)

  lazy val datasetName: String = spec.getDatasetId
  lazy val tableName: String = spec.getTableId
}

package org.mkuthan.streamprocessing.shared.configuration

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers

final case class BigQueryTable[T](id: String) {
  private lazy val tableSpec = BigQueryHelpers.parseTableSpec(id)

  lazy val projectName: String = tableSpec.getProjectId
  lazy val datasetName: String = tableSpec.getDatasetId
  lazy val tableName: String = tableSpec.getTableId
}

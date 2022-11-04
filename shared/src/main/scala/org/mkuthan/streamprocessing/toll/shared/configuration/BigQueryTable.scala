package org.mkuthan.streamprocessing.toll.shared.configuration

import com.spotify.scio.bigquery.Table

final case class BigQueryTable[T](id: String) {
  val spec = Table.Spec(id)
  val datasetName = id.substring(0, id.indexOf('.'))
  val tableName = id.substring(id.indexOf('.') + 1)
}

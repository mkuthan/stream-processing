package org.mkuthan.streamprocessing.shared.configuration

import com.spotify.scio.bigquery.Table

final case class BigQueryTable[T](id: String) extends AnyVal {
  def spec = Table.Spec(id)
  def datasetName = id.substring(0, id.indexOf('.'))
  def tableName = id.substring(id.indexOf('.') + 1)
}

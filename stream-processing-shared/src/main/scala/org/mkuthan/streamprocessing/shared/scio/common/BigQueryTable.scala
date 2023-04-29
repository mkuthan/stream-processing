package org.mkuthan.streamprocessing.shared.scio.common

import com.spotify.scio.bigquery.Table

case class BigQueryTable[T](id: String) extends AnyVal {
  def spec: Table.Spec = Table.Spec(id)
  def datasetName: String = id.substring(0, id.indexOf('.'))
  def tableName: String = id.substring(id.indexOf('.') + 1)
}
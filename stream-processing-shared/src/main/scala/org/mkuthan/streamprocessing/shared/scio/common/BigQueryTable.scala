package org.mkuthan.streamprocessing.shared.scio.common

case class BigQueryTable[T](id: String) {
  lazy val datasetName: String = id.substring(0, id.indexOf('.'))
  lazy val tableName: String = id.substring(id.indexOf('.') + 1)
}

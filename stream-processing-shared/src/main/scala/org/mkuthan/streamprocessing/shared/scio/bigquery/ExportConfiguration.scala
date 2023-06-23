package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

case class ExportConfiguration(
    query: ExportQuery = ExportQuery.NoQuery
) {
  def withQuery(query: ExportQuery): ExportConfiguration =
    copy(query = query)

  def configure[T](tableId: String, read: TypedRead[T]): TypedRead[T] =
    ioParams.foldLeft(read)((read, param) => param.configure(tableId, read))

  private lazy val ioParams: Set[BigQueryReadParam] = Set(
    ExportReadMethod,
    query
  )
}

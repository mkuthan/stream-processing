package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

case class ExportConfiguration(
    query: ExportQuery = ExportQuery.NoQuery
) {
  def withQuery(query: ExportQuery): ExportConfiguration =
    copy(query = query)

  def configure[T](read: TypedRead[T], tableId: String): TypedRead[T] =
    ioParams.foldLeft(read)((read, param) => param.configure(read, tableId))

  private lazy val ioParams: Set[ExportParam] = Set(
    query
  )
}

package org.mkuthan.streamprocessing.infrastructure.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

case class ExportConfiguration() {
  def configure[T](read: TypedRead[T]): TypedRead[T] =
    ioParams.foldLeft(read)((read, param) => param.configure(read))

  private lazy val ioParams: Set[BigQueryReadParam] = Set(
    ExportReadMethod
  )
}

package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

case class FileLoadsConfiguration(
    writeDisposition: WriteDisposition = WriteDisposition.Truncate
) {
  def withWriteDisposition(writeDisposition: WriteDisposition): FileLoadsConfiguration =
    copy(writeDisposition = writeDisposition)

  def configure[T](write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(write))

  private lazy val ioParams: Set[BigQueryWriteParam] = Set(
    FileLoadsWriteMethod,
    CreateDispositionNever,
    writeDisposition
  )
}

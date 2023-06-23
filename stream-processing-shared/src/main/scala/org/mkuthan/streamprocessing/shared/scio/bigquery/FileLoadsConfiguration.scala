package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

case class FileLoadsConfiguration(
    createDisposition: CreateDisposition = CreateDisposition.CreateNever,
    writeDisposition: WriteDisposition = WriteDisposition.WriteEmpty
) {
  def withCreateDisposition(createDisposition: CreateDisposition): FileLoadsConfiguration =
    copy(createDisposition = createDisposition)

  def withWriteDisposition(writeDisposition: WriteDisposition): FileLoadsConfiguration =
    copy(writeDisposition = writeDisposition)

  def configure[T](write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(write))

  private lazy val ioParams: Set[BigQueryWriteParam] = Set(
    createDisposition,
    writeDisposition
  )
}

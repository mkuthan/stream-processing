package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

case class FileLoadsConfiguration(
    createDisposition: CreateDisposition = CreateDisposition.CreateNever,
    writeDisposition: WriteDisposition = WriteDisposition.WriteEmpty,
    triggering: Triggering = Triggering.NoTriggering
) {
  def withCreateDisposition(createDisposition: CreateDisposition): FileLoadsConfiguration =
    copy(createDisposition = createDisposition)

  def withWriteDisposition(writeDisposition: WriteDisposition): FileLoadsConfiguration =
    copy(writeDisposition = writeDisposition)

  def withTriggering(triggering: Triggering): FileLoadsConfiguration =
    copy(triggering = triggering)

  def configure[T](tableId: String, write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(tableId, write))

  private lazy val ioParams: Set[BigQueryWriteParam] = Set(
    WriteTo,
    FileLoadsWriteMethod,
    createDisposition,
    writeDisposition,
    triggering
  )
}

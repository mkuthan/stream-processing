package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

case class StorageWriteConfiguration(
    method: StorageWriteMethod = StorageWriteMethod.AtLeastOnce,
    createDisposition: CreateDisposition = CreateDisposition.CreateNever,
    writeDisposition: WriteDisposition = WriteDisposition.WriteEmpty
) {
  def withCreateDisposition(createDisposition: CreateDisposition): StorageWriteConfiguration =
    copy(createDisposition = createDisposition)

  def withMethod(method: StorageWriteMethod): StorageWriteConfiguration =
    copy(method = method)

  def withWriteDisposition(writeDisposition: WriteDisposition): StorageWriteConfiguration =
    copy(writeDisposition = writeDisposition)

  def configure[T](tableId: String, write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(tableId, write))

  private lazy val ioParams: Set[BigQueryWriteParam] = Set(
    WriteTo,
    method,
    createDisposition,
    writeDisposition
  )
}

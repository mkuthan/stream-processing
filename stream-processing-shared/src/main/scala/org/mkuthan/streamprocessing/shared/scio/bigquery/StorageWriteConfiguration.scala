package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

case class StorageWriteConfiguration(
    createDisposition: CreateDisposition = CreateDisposition.CreateNever,
    method: StorageWriteMethod = StorageWriteMethod.AtLeastOnce,
    writeDisposition: WriteDisposition = WriteDisposition.WriteEmpty
) {
  def withCreateDisposition(createDisposition: CreateDisposition): StorageWriteConfiguration =
    copy(createDisposition = createDisposition)

  def withMethod(method: StorageWriteMethod): StorageWriteConfiguration =
    copy(method = method)

  def withWriteDisposition(writeDisposition: WriteDisposition): StorageWriteConfiguration =
    copy(writeDisposition = writeDisposition)

  def configure[T](write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(write))

  private lazy val ioParams: Set[StorageWriteParam] = Set(
    createDisposition,
    method,
    writeDisposition
  )
}

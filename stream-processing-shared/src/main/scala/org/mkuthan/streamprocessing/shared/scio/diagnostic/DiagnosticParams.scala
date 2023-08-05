package org.mkuthan.streamprocessing.shared.scio.diagnostic

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

sealed trait BigQueryWriteParam {
  def configure[T](write: Write[T]): Write[T]
}

case object WriteDispositionAppend extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
}

case object CreateDispositionNever extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
}

case object StorageWriteAtLeastOnceMethod extends BigQueryWriteParam {
  override def configure[T](write: Write[T]): Write[T] =
    write.withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
}

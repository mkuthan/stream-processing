package org.mkuthan.streamprocessing.infrastructure.pubsub

import scala.annotation.unused

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Write

final case class JsonWriteConfiguration(
    idAttribute: IdAttribute = NoIdAttribute,
    maxBatchBytesSize: MaxBatchBytesSize = MaxBatchBytesSize.HighThroughput,
    maxBatchSize: MaxBatchSize = MaxBatchSize.HighThroughput,
    tsAttribute: TimestampAttribute = NoTimestampAttribute
) {
  def withIdAttribute(idAttribute: IdAttribute): JsonWriteConfiguration =
    copy(idAttribute = idAttribute)

  @unused("how to test batch bytes size?")
  def withMaxBatchBytesSize(maxBatchBytesSize: MaxBatchBytesSize): JsonWriteConfiguration =
    copy(maxBatchBytesSize = maxBatchBytesSize)

  @unused("how to test batch size?")
  def withMaxBatchSize(maxBatchSize: MaxBatchSize): JsonWriteConfiguration =
    copy(maxBatchSize = maxBatchSize)

  def withTimestampAttribute(tsAttribute: TimestampAttribute): JsonWriteConfiguration =
    copy(tsAttribute = tsAttribute)

  def configure[T](write: Write[T]): Write[T] =
    ioParams.foldLeft(write)((write, param) => param.configure(write))

  private lazy val ioParams: Set[PubsubWriteParam] = Set(
    idAttribute,
    maxBatchBytesSize,
    maxBatchSize,
    tsAttribute
  )
}

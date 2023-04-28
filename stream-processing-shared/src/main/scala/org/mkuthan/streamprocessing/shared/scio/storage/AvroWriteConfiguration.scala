package org.mkuthan.streamprocessing.shared.scio.storage

import org.apache.beam.sdk.io.AvroIO

case class AvroWriteConfiguration(
    numShards: NumShards = NumShards.One,
    suffix: Suffix = Suffix.Avro,
    windowedWrites: WindowedWrites = WindowedWritesOn
) {
  def withNumShards(numShards: NumShards): AvroWriteConfiguration =
    copy(numShards = numShards)

  def withSuffix(suffix: Suffix): AvroWriteConfiguration =
    copy(suffix = suffix)

  def withWindowedWrites(windowedWrites: WindowedWrites): AvroWriteConfiguration =
    copy(windowedWrites = windowedWrites)

  def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T] =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[AvroWriteParam] = Set(
    numShards,
    suffix,
    windowedWrites
  )
}

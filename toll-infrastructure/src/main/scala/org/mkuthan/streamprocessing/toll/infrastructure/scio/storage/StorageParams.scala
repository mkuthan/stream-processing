package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.io.TextIO

case class JsonWriteConfiguration(
    numShards: NumShards = NumShards.One,
    suffix: Suffix = Suffix.Json,
    windowedWrites: WindowedWrites = WindowedWritesOn
) {
  def withNumShards(numShards: NumShards): JsonWriteConfiguration =
    copy(numShards = numShards)

  def withSuffix(suffix: Suffix): JsonWriteConfiguration =
    copy(suffix = suffix)

  def withWindowedWrites(windowedWrites: WindowedWrites): JsonWriteConfiguration =
    copy(windowedWrites = windowedWrites)

  def configure(write: TextIO.Write): TextIO.Write =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[TextWriteParam] = Set(
    numShards,
    suffix,
    windowedWrites
  )
}

case class AvroWriteConfiguration(
    numShards: NumShards = NumShards.One,
    suffix: Suffix = Suffix.Json,
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

sealed trait TextWriteParam {
  def configure(write: TextIO.Write): TextIO.Write
}

sealed trait AvroWriteParam {
  def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T]
}

case class NumShards(value: Int) extends TextWriteParam with AvroWriteParam {
  require(value > 0)

  override def configure(write: TextIO.Write): TextIO.Write =
    write.withNumShards(value)

  override def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T] =
    write.withNumShards(value)
}

object NumShards {
  val One = NumShards(1)
}

case class Suffix(value: String) extends TextWriteParam with AvroWriteParam {
  require(!value.isEmpty)

  override def configure(write: TextIO.Write): TextIO.Write =
    write.withSuffix(value)

  override def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T] =
    write.withSuffix(value)
}

object Suffix {
  val Json = Suffix(".json")
  val Avro = Suffix(".avro")
}

sealed trait WindowedWrites extends TextWriteParam with AvroWriteParam

case object WindowedWritesOn extends WindowedWrites {
  override def configure(write: TextIO.Write): TextIO.Write =
    write.withWindowedWrites()

  override def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T] =
    write.withWindowedWrites()
}

case object WindowedWritesOff extends WindowedWrites {
  override def configure(write: TextIO.Write): TextIO.Write =
    write

  override def configure[T](write: AvroIO.Write[T]): AvroIO.Write[T] =
    write
}

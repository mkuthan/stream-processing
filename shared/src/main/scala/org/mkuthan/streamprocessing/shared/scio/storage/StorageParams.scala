package org.mkuthan.streamprocessing.shared.scio.storage

import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.io.TextIO

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

package org.mkuthan.streamprocessing.infrastructure.pubsub

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Write

sealed trait PubsubReadParam {
  def configure[T](read: Read[T]): Read[T]
}

sealed trait PubsubWriteParam {
  def configure[T](write: Write[T]): Write[T]
}

sealed trait IdAttribute extends PubsubReadParam with PubsubWriteParam

case object NoIdAttribute extends IdAttribute {
  override def configure[T](read: Read[T]): Read[T] = read

  override def configure[T](write: Write[T]): Write[T] = write
}

case class NamedIdAttribute(name: String) extends IdAttribute {
  require(name.nonEmpty)

  override def configure[T](read: Read[T]): Read[T] =
    read.withIdAttribute(name)

  override def configure[T](write: Write[T]): Write[T] =
    write.withIdAttribute(name)
}

object NamedIdAttribute {
  val Default: NamedIdAttribute = NamedIdAttribute("id")
}

sealed trait TimestampAttribute extends PubsubReadParam with PubsubWriteParam

case object NoTimestampAttribute extends TimestampAttribute {
  override def configure[T](read: Read[T]): Read[T] = read

  override def configure[T](write: Write[T]): Write[T] = write
}

case class NamedTimestampAttribute(name: String) extends TimestampAttribute {
  require(name.nonEmpty)

  override def configure[T](read: Read[T]): Read[T] =
    read.withTimestampAttribute(name)

  override def configure[T](write: Write[T]): Write[T] =
    write.withTimestampAttribute(name)
}

object NamedTimestampAttribute {
  val Default: NamedTimestampAttribute = NamedTimestampAttribute("ts")
}

case class MaxBatchBytesSize(value: Int) extends PubsubWriteParam {
  require(value > 0)

  override def configure[T](write: Write[T]): Write[T] =
    write.withMaxBatchBytesSize(value)
}

object MaxBatchBytesSize {
  val HighThroughput = MaxBatchBytesSize(8 * 1024 * 1024)
}

case class MaxBatchSize(value: Int) extends PubsubWriteParam {
  require(value > 0)

  override def configure[T](write: Write[T]): Write[T] =
    write.withMaxBatchSize(value)
}

object MaxBatchSize {
  val HighThroughput = MaxBatchSize(1000)
}

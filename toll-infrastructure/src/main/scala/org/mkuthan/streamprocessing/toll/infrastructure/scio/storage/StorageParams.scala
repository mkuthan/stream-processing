package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

import org.apache.beam.sdk.io.TextIO

sealed trait TextWriteParam {
  def configure(write: TextIO.Write): TextIO.Write
}

case class JsonWriteConfiguration(
    numShards: TextNumShards = TextNumShards.One,
    suffix: TextSuffix = TextSuffix.Json,
    windowedWrites: TextWindowedWrites = TextWindowedWritesOn
) {
  def withNumShards(numShards: TextNumShards): JsonWriteConfiguration =
    copy(numShards = numShards)

  def withSuffix(suffix: TextSuffix): JsonWriteConfiguration =
    copy(suffix = suffix)

  def withWindowedWrites(windowedWrites: TextWindowedWrites): JsonWriteConfiguration =
    copy(windowedWrites = windowedWrites)

  def configure(write: TextIO.Write): TextIO.Write =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[TextWriteParam] = Set(
    numShards,
    suffix,
    windowedWrites
  )
}

case class TextNumShards(value: Int) extends TextWriteParam {
  require(value > 0)

  override def configure(write: TextIO.Write): TextIO.Write =
    write.withNumShards(value)
}

object TextNumShards {
  val One = TextNumShards(1)
}

case class TextSuffix(value: String) extends TextWriteParam {
  require(!value.isEmpty)

  override def configure(write: TextIO.Write): TextIO.Write =
    write.withSuffix(value)
}

object TextSuffix {
  val Json = TextSuffix(".json")
}

sealed trait TextWindowedWrites extends TextWriteParam

case object TextWindowedWritesOn extends TextWindowedWrites {
  override def configure(write: TextIO.Write): TextIO.Write =
    write.withWindowedWrites()
}

case object TextWindowedWritesOff extends TextWindowedWrites {
  override def configure(write: TextIO.Write): TextIO.Write =
    write
}

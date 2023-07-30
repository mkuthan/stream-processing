package org.mkuthan.streamprocessing.shared.scio.dlq

import org.apache.beam.sdk.io.TextIO

sealed trait TextWriteParam {
  def configure(write: TextIO.Write): TextIO.Write
}

case class NumShards(value: Int) extends TextWriteParam {
  require(value > 0)

  override def configure(write: TextIO.Write): TextIO.Write =
    write.withNumShards(value)
}

object NumShards {
  val One: NumShards = NumShards(1)
}

case object JsonSuffix extends TextWriteParam {
  override def configure(write: TextIO.Write): TextIO.Write =
    write.withSuffix(".json")
}

case object WindowedWrites extends TextWriteParam {
  override def configure(write: TextIO.Write): TextIO.Write =
    write.withWindowedWrites()
}

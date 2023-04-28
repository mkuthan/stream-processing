package org.mkuthan.streamprocessing.toll.infrastructure.scio.storage

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

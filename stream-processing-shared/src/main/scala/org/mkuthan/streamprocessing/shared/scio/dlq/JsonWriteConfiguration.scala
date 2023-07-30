package org.mkuthan.streamprocessing.shared.scio.dlq

import org.apache.beam.sdk.io.TextIO
import org.joda.time.Duration

case class JsonWriteConfiguration(
    duration: Duration = Duration.standardMinutes(10),
    numShards: NumShards = NumShards.One
) {
  def withDuration(duration: Duration): JsonWriteConfiguration =
    copy(duration = duration)

  def withNumShards(numShards: NumShards): JsonWriteConfiguration =
    copy(numShards = numShards)

  def configure(write: TextIO.Write): TextIO.Write =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[TextWriteParam] = Set(
    JsonSuffix,
    WindowedWrites,
    numShards
  )
}

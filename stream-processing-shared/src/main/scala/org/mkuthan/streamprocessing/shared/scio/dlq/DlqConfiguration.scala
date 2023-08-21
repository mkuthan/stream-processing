package org.mkuthan.streamprocessing.shared.scio.dlq

import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterFirst
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

case class DlqConfiguration(
    prefix: Prefix = Prefix.Empty,
    numShards: NumShards = NumShards.RunnerSpecific,
    windowDuration: Duration = Duration.standardMinutes(10),
    maxRecords: Int = 1_000_000,
    allowedLateness: Duration = Duration.ZERO
) {
  import DlqConfiguration._

  lazy val windowOptions: WindowOptions = createWindowOptions(allowedLateness, maxRecords)

  def withPrefix(prefix: Prefix): DlqConfiguration =
    copy(prefix = prefix)

  def withNumShards(numShards: NumShards): DlqConfiguration =
    copy(numShards = numShards)

  def withWindowDuration(duration: Duration): DlqConfiguration =
    copy(windowDuration = duration)

  def withMaxRecords(maxRecords: Int): DlqConfiguration =
    copy(maxRecords = maxRecords)

  def withAllowedLateness(duration: Duration): DlqConfiguration =
    copy(allowedLateness = duration)

  def configure(write: StorageWriteParam.Type): StorageWriteParam.Type =
    params.foldLeft(write)((write, param) => param.configure(write))

  private lazy val params: Set[StorageWriteParam] = Set(
    JsonSuffix,
    prefix,
    numShards
  )
}

object DlqConfiguration {
  private def createWindowOptions(allowedLateness: Duration, elementCount: Int): WindowOptions =
    WindowOptions(
      trigger = Repeatedly.forever(
        AfterFirst.of(
          AfterWatermark.pastEndOfWindow(),
          AfterPane.elementCountAtLeast(elementCount)
        )
      ),
      allowedLateness = allowedLateness,
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      onTimeBehavior = Window.OnTimeBehavior.FIRE_IF_NON_EMPTY
    )
}

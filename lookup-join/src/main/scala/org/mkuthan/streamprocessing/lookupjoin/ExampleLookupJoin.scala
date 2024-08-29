package org.mkuthan.streamprocessing.lookupjoin

import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration

object ExampleLookupJoin {

  case class Value(id: String, name: String)

  case class Lookup(id: String, name: String)

  def lookupJoin(
      values: SCollection[Value],
      lookups: SCollection[Lookup],
      valuesTimeToLive: Duration,
      lookupTimeToLive: Duration,
      accumulationMode: AccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      closingBehavior: ClosingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
      timestampCombiner: TimestampCombiner = TimestampCombiner.LATEST
  ): SCollection[(Value, Option[Lookup])] = {
    val globalWindowOptions = WindowOptions(
      trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
      accumulationMode = accumulationMode,
      closingBehavior = closingBehavior,
      timestampCombiner = timestampCombiner
    )

    val valuesById = values
      .withGlobalWindow(globalWindowOptions)
      .keyBy(_.id)

    val lookupById = lookups
      .withGlobalWindow(globalWindowOptions)
      .keyBy(_.id)

    valuesById
      .cogroup(lookupById)
      .applyPerKeyDoFn(new LookupJoinDoFn(valuesTimeToLive, lookupTimeToLive))
      .values
  }
}

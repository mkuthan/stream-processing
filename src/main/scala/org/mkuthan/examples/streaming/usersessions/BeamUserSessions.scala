package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Trigger
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.joda.time.Instant

object BeamUserSessions {

  type User = String
  type Action = String

  def activitiesInSessionWindow(
      userActions: SCollection[(User, Action)],
      gapDuration: Duration,
      allowedLateness: Duration = Duration.ZERO,
      accumulationMode: AccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      timestampCombiner: TimestampCombiner = TimestampCombiner.END_OF_WINDOW,
      trigger: Trigger = DefaultTrigger.of()
  ): SCollection[(User, Iterable[Action])] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      accumulationMode = accumulationMode,
      timestampCombiner = timestampCombiner,
      trigger = trigger,
    )

    userActions
      .withTimestamp
      .map { case ((user, action), timestamp) => (user, (action, timestamp)) }
      .withSessionWindows(gapDuration, windowOptions)
      .groupByKey
      .mapValues(sort)
      .mapValues(withoutTimestamp)
  }

  private def sort(actions: Iterable[(Action, Instant)]): Iterable[(Action, Instant)] = {
    val ordering: Ordering[(Action, Instant)] = Ordering.by { case (_, instant) => instant.getMillis }
    actions.toSeq.sorted(ordering)
  }

  private def withoutTimestamp(actions: Iterable[(Action, Instant)]): Iterable[Action] =
    actions.map(_._1)
}

package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger
import org.apache.beam.sdk.transforms.windowing.Trigger
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.joda.time.Instant

object BeamUserSessions {

  type User = String
  type Activity = String

  def activitiesInSessionWindow(
      activities: SCollection[(User, Activity)],
      gapDuration: Duration,
      allowedLateness: Duration = Duration.ZERO,
      accumulationMode: AccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      trigger: Trigger = DefaultTrigger.of()
  ): SCollection[(User, Iterable[Activity])] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      accumulationMode = accumulationMode,
      trigger = trigger
    )

    activities
      .withTimestamp
      .map { case ((user, action), timestamp) => (user, (action, timestamp)) }
      .withSessionWindows(gapDuration, windowOptions)
      .groupByKey
      .mapValues(sortByTimestamp)
      .mapValues(withoutTimestamp)
  }

  private def sortByTimestamp(activities: Iterable[(Activity, Instant)]): Iterable[(Activity, Instant)] = {
    val ordering: Ordering[(Activity, Instant)] = Ordering.by { case (_, instant) => instant.getMillis }
    activities.toSeq.sorted(ordering)
  }

  private def withoutTimestamp(activities: Iterable[(Activity, Instant)]): Iterable[Activity] =
    activities.map(_._1)
}

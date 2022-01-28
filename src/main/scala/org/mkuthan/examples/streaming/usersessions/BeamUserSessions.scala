package org.mkuthan.examples.streaming.usersessions

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

object BeamUserSessions {

  type User = String
  type Action = String

  def activitiesInSessionWindow(
      userActions: SCollection[(User, Action)],
      gapDuration: Duration,
      allowedLateness: Duration = Duration.ZERO,
      accumulationMode: AccumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
  ): SCollection[(User, Iterable[Action])] = {
    val windowOptions = WindowOptions(
      allowedLateness = allowedLateness,
      accumulationMode = accumulationMode
    )

    userActions
      .withSessionWindows(gapDuration, windowOptions)
      .groupByKey
  }
}

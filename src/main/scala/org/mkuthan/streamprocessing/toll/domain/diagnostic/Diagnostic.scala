package org.mkuthan.streamprocessing.toll.domain.diagnostic

import com.spotify.scio.values.SCollection
import org.joda.time.Duration

case class Diagnostic()

object Diagnostic {
  def aggregateInFixedWindow(input: Seq[SCollection[Diagnostic]], duration: Duration): SCollection[Diagnostic] = ???
}

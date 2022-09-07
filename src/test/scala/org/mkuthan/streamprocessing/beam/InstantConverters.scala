package org.mkuthan.streamprocessing.beam

import org.joda.time.Instant
import org.joda.time.LocalTime

private[beam] object InstantConverters {

  private val BaseTime = new Instant(0)

  def stringToInstant(time: String): Instant =
    LocalTime
      .parse(time)
      .toDateTime(BaseTime)
      .toInstant
}

package org.mkuthan.streamprocessing.shared.test

import scala.util.Try

import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalTime

private[test] object InstantConverters {

  private val BaseTime = new Instant(0)

  def stringToInstant(time: String): Instant =
    Try(
      LocalTime
        .parse(time)
        .toDateTime(BaseTime)
        .toInstant
    ).getOrElse(
      DateTime
        .parse(time)
        .toDateTime
        .toInstant
    )
}
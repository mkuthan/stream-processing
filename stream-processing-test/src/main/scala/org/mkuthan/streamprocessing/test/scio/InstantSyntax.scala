package org.mkuthan.streamprocessing.test.scio

import scala.util.Try

import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalTime

private[scio] trait InstantSyntax {
  implicit class StringInstantOps(private val self: String) {
    def toInstant: Instant = {
      val localTime = Try {
        LocalTime
          .parse(self)
          .toDateTime(Instant.EPOCH)
          .toInstant
      }
      localTime.getOrElse {
        DateTime
          .parse(self)
          .toInstant
      }
    }
  }
}

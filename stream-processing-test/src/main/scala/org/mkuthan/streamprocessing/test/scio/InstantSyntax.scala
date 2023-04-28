package org.mkuthan.streamprocessing.test.scio

import scala.language.implicitConversions
import scala.util.Try

import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalTime

private[scio] class StringOps(private val s: String) extends AnyVal {
  def toInstant: Instant = {
    val localTime = Try(LocalTime.parse(s).toDateTime(Instant.EPOCH).toInstant)
    localTime.getOrElse(DateTime.parse(s).toInstant)
  }
}

private[scio] trait InstantSyntax {
  implicit def instantStringOps(s: String): StringOps = new StringOps(s)
}

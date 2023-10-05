package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.values.SCollection

import cats.kernel.Eq
import org.joda.time.Instant
import org.scalatest.matchers.Matcher

// experimental, new matchers
trait ScioMatchers extends InstantSyntax {

  import scala.language.implicitConversions

  // smart or too smart?
  implicit def toTimestampedSCollection[T: Coder](sc: SCollection[T]): SCollection[(T, Instant)] = sc.withTimestamp

  private def matcher = new SCollectionMatchers {}

  def containsAtTime[T: Coder](
      time: String,
      element: T,
      elements: T*
  )(implicit eq: Eq[(T, Instant)]): Matcher[SCollection[(T, Instant)]] =
    containsAtTime(time.toInstant, element, elements: _*)

  def containsAtTime[T: Coder](
      time: Instant,
      element: T,
      elements: T*
  )(implicit eq: Eq[(T, Instant)]): Matcher[SCollection[(T, Instant)]] = {
    val timestampedElements = (element +: elements).map(e => (e, time))
    matcher.containInAnyOrder(timestampedElements)
  }
}

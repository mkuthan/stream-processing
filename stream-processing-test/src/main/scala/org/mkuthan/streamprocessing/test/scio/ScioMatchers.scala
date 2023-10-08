package org.mkuthan.streamprocessing.test.scio

import scala.reflect.ClassTag

import org.apache.beam.sdk.transforms.windowing.IntervalWindow

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.EqInstances
import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.values.SCollection

import org.joda.time.Instant
import org.scalatest.matchers.Matcher

import org.mkuthan.streamprocessing.test.common.InstantSyntax

// experimental, new matchers
trait ScioMatchers extends EqInstances with InstantSyntax {

  private val matcher = new SCollectionMatchers {}

  def containsAtTime[T: Coder](
      time: String,
      element: T,
      elements: T*
  ): Matcher[SCollection[(T, Instant)]] =
    containsAtTime(time.toInstant, element, elements: _*)

  def containsAtTime[T: Coder](
      time: Instant,
      element: T,
      elements: T*
  ): Matcher[SCollection[(T, Instant)]] = {
    val timestampedElements = (element +: elements).map(e => (e, time))
    matcher.containInAnyOrder(timestampedElements)
  }

//  def beEmpty =
//    matcher.beEmpty

  def inWindow[T: ClassTag](begin: String, end: String)(m: matcher.IterableMatcher[T, T]): Matcher[T] = {
    val window = new IntervalWindow(begin.toInstant, end.toInstant)
    m.matcher(_.inWindow(window))
  }
}

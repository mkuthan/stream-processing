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

trait ScioMatchers extends InstantSyntax with EqInstances {

  final val m: SCollectionMatchers = new SCollectionMatchers {}

  def containElements[T: Coder](
      element: T,
      elements: T*
  ): m.IterableMatcher[SCollection[T], T] = {
    val all = (element +: elements)
    m.containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder](
      time: String,
      element: T,
      elements: T*
  ): m.IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map(e => (e, time.toInstant))
    m.containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder](
      time: Instant,
      element: T,
      elements: T*
  ): m.IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map(e => (e, time))
    m.containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder](
      element: (String, T),
      elements: (String, T)*
  ): m.IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map { case (t, e) => (e, t.toInstant) }
    m.containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder](
      element: (Instant, T),
      elements: (Instant, T)*
  )(implicit d: DummyImplicit): m.IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map { case (t, e) => (e, t) }
    m.containInAnyOrder(all)
  }

  val beEmpty: m.IterableMatcher[SCollection[_], Any] = m.beEmpty

  def haveSize(size: Int): m.IterableMatcher[SCollection[_], Any] = m.haveSize(size)

  def inWindow[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] = {
    val window = new IntervalWindow(begin.toInstant, end.toInstant)
    matcher match {
      case value: m.SingleMatcher[_, _] =>
        value.matcher(_.inWindow(window))
      case value: m.IterableMatcher[_, _] =>
        value.matcher(_.inWindow(window))
    }
  }

  def inOnTimePane[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] =
    m.inOnTimePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inLatePane[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] =
    m.inLatePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inEarlyPane[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] =
    m.inEarlyPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inFinalPane[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] =
    m.inFinalPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inOnlyPane[T: ClassTag](begin: String, end: String)(matcher: m.MatcherBuilder[T]): Matcher[T] = {
    val window = new IntervalWindow(begin.toInstant, end.toInstant)
    matcher match {
      case value: m.SingleMatcher[_, _] =>
        value.matcher(_.inOnlyPane(window))
      case value: m.IterableMatcher[_, _] =>
        value.matcher(_.inOnlyPane(window))
    }
  }

}

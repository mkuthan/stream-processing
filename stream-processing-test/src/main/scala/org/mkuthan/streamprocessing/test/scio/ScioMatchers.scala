package org.mkuthan.streamprocessing.test.scio

import scala.reflect.ClassTag

import org.apache.beam.sdk.transforms.windowing.IntervalWindow

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.values.SCollection

import cats.kernel.Eq
import org.joda.time.Instant
import org.scalatest.matchers.Matcher

import org.mkuthan.streamprocessing.test.common.InstantSyntax

trait ScioMatchers extends InstantSyntax {
  this: SCollectionMatchers =>

  def containElements[T: Coder: Eq](
      element: T,
      elements: T*
  ): IterableMatcher[SCollection[T], T] = {
    val all = (element +: elements)
    containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder: Eq](
      time: String,
      element: T,
      elements: T*
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map(e => (e, time.toInstant))
    containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder: Eq](
      time: Instant,
      element: T,
      elements: T*
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map(e => (e, time))
    containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder: Eq](
      element: (String, T),
      elements: (String, T)*
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map { case (t, e) => (e, t.toInstant) }
    containInAnyOrder(all)
  }

  def containElementsAtTime[T: Coder: Eq](
      element: (Instant, T),
      elements: (Instant, T)*
  )(implicit d: DummyImplicit): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] = {
    val all = (element +: elements).map { case (t, e) => (e, t) }
    containInAnyOrder(all)
  }

  def inWindow[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] = {
    val window = new IntervalWindow(begin.toInstant, end.toInstant)
    matcher match {
      case value: SingleMatcher[_, _] =>
        value.matcher(_.inWindow(window))
      case value: IterableMatcher[_, _] =>
        value.matcher(_.inWindow(window))
    }
  }

  def inOnTimePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inOnTimePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inLatePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inLatePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inEarlyPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inEarlyPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inFinalPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inFinalPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inOnlyPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] = {
    val window = new IntervalWindow(begin.toInstant, end.toInstant)
    matcher match {
      case value: SingleMatcher[_, _] =>
        value.matcher(_.inOnlyPane(window))
      case value: IterableMatcher[_, _] =>
        value.matcher(_.inOnlyPane(window))
    }
  }

}

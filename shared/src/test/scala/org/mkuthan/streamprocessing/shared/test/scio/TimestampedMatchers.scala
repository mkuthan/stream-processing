package org.mkuthan.streamprocessing.shared.test.scio

import scala.reflect.ClassTag

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.SCollectionMatchers
import com.spotify.scio.values.SCollection

import cats.kernel.Eq
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.Instant
import org.scalatest.matchers.Matcher

trait TimestampedMatchers extends InstantSyntax {
  this: SCollectionMatchers =>

  def inOnTimePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inOnTimePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inLatePane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inLatePane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inEarlyPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inEarlyPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inFinalPane[T: ClassTag](begin: String, end: String)(matcher: MatcherBuilder[T]): Matcher[T] =
    inFinalPane(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  def inWindow[T: ClassTag, B: ClassTag](begin: String, end: String)(matcher: IterableMatcher[T, B]): Matcher[T] =
    inWindow(new IntervalWindow(begin.toInstant, end.toInstant))(matcher)

  /**
   * Assert that the SCollection contains the provided element at given time without making assumptions about other
   * elements in the collection.
   */
  def containValueAtTime[T: Coder: Eq](
      time: String,
      value: T
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containValue((value, time.toInstant))

  /**
   * Assert that the SCollection contains a single provided element at given time.
   */
  def containSingleValueAtTime[T: Coder: Eq](
      time: String,
      value: T
  ): SingleMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containSingleValue((value, time.toInstant))

  /**
   * Assert that the SCollection contains the provided elements at given time.
   */
  def containInAnyOrderAtTime[T: Coder: Eq](
      time: String,
      value: Iterable[T]
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containInAnyOrder(value.map { case v => (v, time.toInstant) })

  /**
   * Assert that the SCollection contains the provided timestamped elements.
   */
  def containInAnyOrderAtTime[T: Coder: Eq](
      value: Iterable[(String, T)]
  ): IterableMatcher[SCollection[(T, Instant)], (T, Instant)] =
    containInAnyOrder(value.map { case (time, v) => (v, time.toInstant) })
}

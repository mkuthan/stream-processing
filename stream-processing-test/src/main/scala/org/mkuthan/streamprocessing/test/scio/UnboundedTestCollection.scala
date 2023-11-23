package org.mkuthan.streamprocessing.test.scio

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.test.common.InstantSyntax
import org.mkuthan.streamprocessing.test.common.RandomString

final case class UnboundedTestCollection[T: Coder](name: String, testStream: TestStream[T])

object UnboundedTestCollection {

  def builder[T: Coder](): Builder[T] = {
    val testStream = TestStream.create(CoderMaterializer.beamWithDefault(Coder[T]))
    Builder(testStream)
  }

  final case class Builder[T: Coder](builder: TestStream.Builder[T]) extends InstantSyntax {
    def addElementsAtWatermarkTime(elements: T*): Builder[T] =
      elements match {
        case Seq(x, xs @ _*) => Builder(builder.addElements(x, xs: _*))
        case _               => this
      }

    def addElementsAtTime(time: String, elements: T*): Builder[T] =
      addElementsAtTime(time.toInstant, elements: _*)

    def addElementsAtTime(instant: Instant, elements: T*): Builder[T] = {
      val timestampedElements = elements.map(e => TimestampedValue.of(e, instant))
      timestampedElements match {
        case Seq(x, xs @ _*) => Builder(builder.addElements(x, xs: _*))
        case _               => this
      }
    }

    def advanceProcessingTime(duration: Duration): Builder[T] =
      Builder(builder.advanceProcessingTime(duration))

    def advanceWatermarkTo(time: String): Builder[T] =
      advanceWatermarkTo(time.toInstant)

    def advanceWatermarkTo(instant: Instant): Builder[T] =
      Builder(builder.advanceWatermarkTo(instant))

    def advanceWatermarkToInfinity(): UnboundedTestCollection[T] = {
      val testStream = builder.advanceWatermarkToInfinity()

      UnboundedTestCollection(RandomString.randomString(), testStream)
    }
  }
}

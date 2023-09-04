package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Duration
import org.joda.time.Instant

case class UnboundedTestCollection[T: Coder](name: String, testStream: TestStream[T])

object UnboundedTestCollection {

  def builder[T: Coder](): Builder[T] = {
    val testStream = TestStream.create(CoderMaterializer.beamWithDefault(Coder[T]))
    Builder(testStream)
  }

  case class Builder[T: Coder](builder: TestStream.Builder[T]) extends InstantSyntax {
    def addElementsAtWatermarkTime(element: T, elements: T*): Builder[T] =
      Builder(builder.addElements(element, elements: _*))

    def addElementsAtTime(time: String, element: T, elements: T*): Builder[T] =
      addElementsAtTime(time.toInstant, element, elements: _*)

    def addElementsAtTime(instant: Instant, element: T, elements: T*): Builder[T] = {
      val timestampedElement = TimestampedValue.of(element, instant)
      val timestampedElements = elements.map(e => TimestampedValue.of(e, instant))

      Builder(builder.addElements(timestampedElement, timestampedElements: _*))
    }

    def advanceProcessingTime(duration: Duration): Builder[T] =
      Builder(builder.advanceProcessingTime(duration))

    def advanceWatermarkTo(time: String): Builder[T] =
      advanceWatermarkTo(time.toInstant)

    def advanceWatermarkTo(instant: Instant): Builder[T] =
      Builder(builder.advanceWatermarkTo(instant))

    def advanceWatermarkToInfinity(): UnboundedTestCollection[T] = {
      val testStream = builder.advanceWatermarkToInfinity()

      // TODO: make transform name unique and avoid warnings from direct runner
      UnboundedTestCollection("foo", testStream)
    }
  }
}

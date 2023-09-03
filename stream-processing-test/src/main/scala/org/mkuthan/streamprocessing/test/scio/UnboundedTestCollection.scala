package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Duration

// TODO: make it more private to scio package

case class UnboundedTestCollection[T: Coder](name: String, testStream: TestStream[T])

object UnboundedTestCollection {

  def builder[T: Coder](): Builder[T] = {
    val testStream = TestStream.create(CoderMaterializer.beamWithDefault(Coder[T]))
    Builder(testStream)
  }

  case class Builder[T: Coder](builder: TestStream.Builder[T]) extends InstantSyntax {
    def addElementsAtMinimumTime(element: T, elements: T*): Builder[T] = {
      val timestampedElement = TimestampedValue.atMinimumTimestamp(element)
      val timestampedElements = elements.map(e => TimestampedValue.atMinimumTimestamp(e))

      Builder(builder.addElements(timestampedElement, timestampedElements: _*))
    }

    def addElementsAtTime(time: String, element: T, elements: T*): Builder[T] = {
      val instant = time.toInstant

      val timestampedElement = TimestampedValue.of(element, instant)
      val timestampedElements = elements.map(e => TimestampedValue.of(e, instant))

      Builder(builder.addElements(timestampedElement, timestampedElements: _*))
    }

    // TODO: Joda or Scala duration?
    def advanceProcessingTime(duration: Duration): Builder[T] =
      Builder(builder.advanceProcessingTime(duration))

    def advanceWatermarkTo(time: String): Builder[T] = {
      val instant = time.toInstant
      Builder(builder.advanceWatermarkTo(instant))
    }

    def advanceWatermarkToInfinity(): UnboundedTestCollection[T] = {
      val testStream = builder.advanceWatermarkToInfinity()

      // TODO: make transform name unique and avoid warnings from direct runner
      UnboundedTestCollection("foo", testStream)
    }
  }
}

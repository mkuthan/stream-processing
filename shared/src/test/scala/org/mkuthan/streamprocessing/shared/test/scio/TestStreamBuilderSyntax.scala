package org.mkuthan.streamprocessing.shared.test.scio

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Instant

private[scio] trait TestStreamBuilderSyntax {

  implicit class TestStreamBuilderOps[T](builder: TestStream.Builder[T]) extends InstantSyntax {
    def addElementsAtTime(time: Instant, element: T, elements: T*): TestStream.Builder[T] = {
      val timestampedElement = TimestampedValue.of(element, time)
      val timestampedElements = elements.map(TimestampedValue.of(_, time))
      builder.addElements(timestampedElement, timestampedElements: _*)
    }

    def addElementsAtTime(time: String, element: T, elements: T*): TestStream.Builder[T] =
      addElementsAtTime(time.toInstant, element, elements: _*)

    def advanceWatermarkTo(time: String): TestStream.Builder[T] =
      builder.advanceWatermarkTo(time.toInstant)
  }
}

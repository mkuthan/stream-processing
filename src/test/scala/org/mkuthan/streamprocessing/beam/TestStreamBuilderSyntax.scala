package org.mkuthan.streamprocessing.beam

import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue

private[beam] trait TestStreamBuilderSyntax {

  import InstantConverters._

  implicit class TestStreamBuilderOps[T](builder: TestStream.Builder[T]) {

    def addElementsAtTime(time: String, element: T, elements: T*): TestStream.Builder[T] = {
      val timestampedElement = TimestampedValue.of(element, stringToInstant(time))
      val timestampedElements = elements.map(TimestampedValue.of(_, stringToInstant(time)))
      builder.addElements(timestampedElement, timestampedElements: _*)
    }

    def advanceWatermarkTo(time: String): TestStream.Builder[T] =
      builder.advanceWatermarkTo(stringToInstant(time))
  }

}

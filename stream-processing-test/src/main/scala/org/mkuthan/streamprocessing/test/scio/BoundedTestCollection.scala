package org.mkuthan.streamprocessing.test.scio

import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.TimestampedValue

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer

import org.joda.time.Instant

import org.mkuthan.streamprocessing.test.common.InstantSyntax
import org.mkuthan.streamprocessing.test.common.RandomString

final case class BoundedTestCollection[T: Coder](name: String, timestampedValues: Create.TimestampedValues[T])

object BoundedTestCollection extends InstantSyntax {

  def builder[T: Coder](): Builder[T] =
    Builder[T](Seq.empty)

  final case class Builder[T: Coder](content: Seq[TimestampedValue[T]]) {
    def addElementsAtMinimumTime(element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.atMinimumTimestamp(e))
      Builder(timestampedElements)
    }

    def addElementsAtTime(time: String, element: T, elements: T*): Builder[T] =
      addElementsAtTime(time.toInstant, element, elements: _*)

    def addElementsAtTime(instant: Instant, element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.of(e, instant))
      Builder(timestampedElements)
    }

    def advanceWatermarkToInfinity(): BoundedTestCollection[T] = {
      import scala.jdk.CollectionConverters._

      val coder = CoderMaterializer.beamWithDefault(Coder[T])
      val timestampedValues = Create.timestamped(content.asJava).withCoder(coder)

      BoundedTestCollection[T](RandomString.randomString(), timestampedValues)
    }
  }
}

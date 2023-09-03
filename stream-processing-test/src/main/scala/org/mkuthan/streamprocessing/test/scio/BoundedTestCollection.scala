package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer

import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.TimestampedValue

// TODO: make it more private to scio package

case class BoundedTestCollection[T: Coder](name: String, timestampedValues: Create.TimestampedValues[T])

object BoundedTestCollection extends InstantSyntax {

  def builder[T: Coder](): Builder[T] =
    Builder[T](Seq.empty)

  case class Builder[T: Coder](content: Seq[TimestampedValue[T]]) {

    def addElementsAtMinimumTime(element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.atMinimumTimestamp(e))
      Builder(timestampedElements)
    }

    def addElementsAtTime(time: String, element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.of(e, time.toInstant))
      Builder(timestampedElements)
    }

    def build(): BoundedTestCollection[T] = {
      import scala.jdk.CollectionConverters._

      val coder = CoderMaterializer.beamWithDefault(Coder[T])
      val timestampedValues = Create.timestamped(content.asJava).withCoder(coder)

      // TODO: make transform name unique and avoid warnings from direct runner
      new BoundedTestCollection[T]("foo", timestampedValues)
    }
  }
}

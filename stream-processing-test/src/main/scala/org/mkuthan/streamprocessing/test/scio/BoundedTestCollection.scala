package org.mkuthan.streamprocessing.test.scio

import org.apache.beam.sdk.values.TimestampedValue

case class BoundedTestCollection[T](content: Seq[TimestampedValue[T]])

object BoundedTestCollection extends InstantSyntax {

  def builder[T](): Builder[T] =
    Builder[T](Seq.empty)

  case class Builder[T](content: Seq[TimestampedValue[T]]) {

    def addElements(element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.atMinimumTimestamp(e))
      Builder(timestampedElements)
    }

    def addElements(time: String, element: T, elements: T*): Builder[T] = {
      val timestampedElements = (element +: elements)
        .foldLeft(content)((acc, e) => acc :+ TimestampedValue.of(e, time.toInstant))
      Builder(timestampedElements)
    }

    def build(): BoundedTestCollection[T] = new BoundedTestCollection[T](content) {}
  }
}

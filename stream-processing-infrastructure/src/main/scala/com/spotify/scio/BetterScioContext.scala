package com.spotify.scio

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PInput

object BetterScioContext {

  implicit class BetterScioContextOps(val self: ScioContext) {
def betterCustomInput[T, I >: PBegin <: PInput](name: String)(transformFn: I => SCollection[T]): SCollection[T] =
  self.requireNotClosed {
    if (self.isTest) {
      TestDataManager.getInput(self.testId.get)(CustomIO[T](name)).toSCollection(self)
    } else {
      self.applyTransform(
        name,
        new PTransform[I, PCollection[T]]() {
          override def expand(input: I): PCollection[T] =
            transformFn(input).internal
        }
      )
    }
  }
}
}

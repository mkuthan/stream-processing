package com.spotify.scio.values

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.io.CustomIO
import com.spotify.scio.io.EmptyTap
import com.spotify.scio.testing.TestDataManager
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.POutput

object BetterSCollection {

  implicit class BetterSCollectionOps[T](val self: SCollection[T]) {
def betterSaveAsCustomOutput[O <: POutput](name: String)(transformFn: SCollection[T] => O): ClosedTap[Nothing] = {
  if (self.context.isTest) {
    TestDataManager.getOutput(self.context.testId.get)(CustomIO[T](name))(self)
  } else {
    self.applyInternal(
      name,
      new PTransform[PCollection[T], O]() {
        override def expand(input: PCollection[T]): O =
          transformFn(self.context.wrap(input))
      }
    )
  }

  ClosedTap[Nothing](EmptyTap)
}
}
}

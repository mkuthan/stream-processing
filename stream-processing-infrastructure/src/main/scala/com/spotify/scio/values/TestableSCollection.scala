package com.spotify.scio.values

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.POutput

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.io.CustomIO
import com.spotify.scio.io.EmptyTap
import com.spotify.scio.testing.TestDataManager

// https://github.com/spotify/scio/issues/4995
object TestableSCollection {

  implicit class TestableSCollectionOps[T](val self: SCollection[T]) {
    def testableSaveAsCustomOutput[O <: POutput](name: String)(transformFn: SCollection[T] => O): ClosedTap[Nothing] = {
      if (self.context.isTest) {
        TestDataManager.getOutput(self.context.testId.getOrElse(""))(CustomIO[T](name))(self)
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

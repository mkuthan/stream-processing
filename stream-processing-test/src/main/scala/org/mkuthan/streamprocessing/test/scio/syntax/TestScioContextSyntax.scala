package org.mkuthan.streamprocessing.test.scio.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.test.scio.BoundedTestCollection
import org.mkuthan.streamprocessing.test.scio.UnboundedTestCollection

private[syntax] trait TestScioContextSyntax {
  implicit class TestScioContextOps(private val self: ScioContext) {
    def testBounded[T: Coder](input: BoundedTestCollection[T]): SCollection[T] = {
      val pCollection = self.pipeline.apply(input.name, input.timestampedValues)
      self.wrap(pCollection)
    }

    def testUnbounded[T: Coder](input: UnboundedTestCollection[T]): SCollection[T] = {
      val pCollection = self.pipeline.apply(input.name, input.testStream)
      self.wrap(pCollection)
    }
  }
}

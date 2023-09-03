package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

private[scio] class TestCollectionOps(private val self: ScioContext) extends AnyVal {
  def testBounded[T: Coder](input: BoundedTestCollection[T]): SCollection[T] = {
    val pCollection = self.pipeline.apply(input.name, input.timestampedValues)
    self.wrap(pCollection)
  }

  def testUnbounded[T: Coder](input: UnboundedTestCollection[T]): SCollection[T] = {
    val pCollection = self.pipeline.apply(input.name, input.testStream)
    self.wrap(pCollection)
  }
}

trait TestCollectionSyntax {

  import scala.language.implicitConversions

  implicit def testCollectionOps(sc: ScioContext): TestCollectionOps = new TestCollectionOps(sc)
}

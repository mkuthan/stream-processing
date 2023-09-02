package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

private[scio] class TestCollectionOps(private val self: ScioContext) extends AnyVal {
  def test[T: Coder](input: BoundedTestCollection[T]): SCollection[T] =
    self.parallelizeTimestamped(
      input.content
        .map(timestamped => (timestamped.getValue, timestamped.getTimestamp))
    )

  def test[T: Coder](input: UnboundedTestCollection[T]): SCollection[T] = {
    val pCollection = self.pipeline.apply(input.testStream)
    self.wrap(pCollection)
  }
}

trait TestCollectionSyntax {
  import scala.language.implicitConversions

  implicit def testCollectionOps(sc: ScioContext): TestCollectionOps = new TestCollectionOps(sc)
}

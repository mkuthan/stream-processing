package org.mkuthan.streamprocessing.test.scio

import com.spotify.scio.coders.Coder

package object syntax extends TestSyntax {
  def boundedTestCollectionOf[T: Coder]: BoundedTestCollection.Builder[T] = BoundedTestCollection.builder()
  def unboundedTestCollectionOf[T: Coder]: UnboundedTestCollection.Builder[T] = UnboundedTestCollection.builder()
}

package org.mkuthan.streamprocessing.test

import com.spotify.scio.coders.Coder

package object scio {
  def boundedTestCollectionOf[T: Coder]: BoundedTestCollection.Builder[T] = BoundedTestCollection.builder()
  def unboundedTestCollectionOf[T: Coder]: UnboundedTestCollection.Builder[T] = UnboundedTestCollection.builder()
}

package org.mkuthan.streamprocessing.shared.scio.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

private[syntax] trait ScioContextSyntax {

  implicit class ScioContextOps(private val self: ScioContext) extends SCollectionSyntax {
    def unionInGlobalWindow[T: Coder](
        first: SCollection[T],
        others: SCollection[T]*
    ): SCollection[T] =
      first.unionInGlobalWindow(others: _*)
  }
}

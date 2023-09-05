package org.mkuthan.streamprocessing.shared.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

trait ScioContextSyntax {

  import scala.language.implicitConversions

  implicit def sharedScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}

private[scio] class ScioContextOps(private val self: ScioContext) extends SCollectionSyntax {
  def unionInGlobalWindow[T: Coder](
      first: SCollection[T],
      others: SCollection[T]*
  ): SCollection[T] =
    first.unionInGlobalWindow(others: _*)
}

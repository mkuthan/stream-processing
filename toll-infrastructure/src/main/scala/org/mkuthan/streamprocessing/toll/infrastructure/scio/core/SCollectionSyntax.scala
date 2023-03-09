package org.mkuthan.streamprocessing.toll.infrastructure.scio.core

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput

private[core] final class SCollectionEitherOps[L: Coder, R: Coder](private val self: SCollection[Either[L, R]]) {
  def unzip: (SCollection[R], SCollection[L]) = {
    val leftOutput = SideOutput[L]()

    val (rightOutput, sideOutputs) = self
      .withSideOutputs(leftOutput)
      .flatMap {
        case (Right(r), _) => Some(r)
        case (Left(l), ctx) =>
          ctx.output(leftOutput, l)
          None
      }

    (rightOutput, sideOutputs(leftOutput))
  }
}

trait SCollectionSyntax {
  implicit def coreSCollectionEitherOps[L: Coder, R: Coder](sc: SCollection[Either[L, R]]): SCollectionEitherOps[L, R] =
    new SCollectionEitherOps(sc)
}
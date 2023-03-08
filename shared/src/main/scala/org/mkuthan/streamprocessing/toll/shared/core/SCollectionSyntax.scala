package org.mkuthan.streamprocessing.toll.shared.core

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput

object SCollectionSyntax {
  implicit class SCollectionEitherOps[L: Coder, R: Coder](private val self: SCollection[Either[L, R]]) {
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
}

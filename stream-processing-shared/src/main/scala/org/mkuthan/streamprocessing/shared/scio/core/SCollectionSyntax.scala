package org.mkuthan.streamprocessing.shared.scio.core

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput

private[core] class SCollectionEitherOps[L: Coder, R: Coder](private val self: SCollection[Either[L, R]]) {
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

private[core] class SCollectionOps[T: Coder](private val self: SCollection[T]) {
  def metrics(counter: Counter[T]): SCollection[T] =
    self.tap(_ => counter.inc())
}

trait SCollectionSyntax {
  implicit def coreSCollectionOps[T: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)

  implicit def coreSCollectionEitherOps[L: Coder, R: Coder](sc: SCollection[Either[L, R]]): SCollectionEitherOps[L, R] =
    new SCollectionEitherOps(sc)
}

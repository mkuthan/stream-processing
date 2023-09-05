package org.mkuthan.streamprocessing.shared.scio

import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions

trait SCollectionSyntax {

  import scala.language.implicitConversions

  implicit def sharedSCollectionOps[T: Coder](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)

  implicit def sharedSCollectionEitherOps[L: Coder, R: Coder](sc: SCollection[Either[L, R]])
      : SCollectionEitherOps[L, R] =
    new SCollectionEitherOps(sc)
}

private[scio] class SCollectionOps[T: Coder](private val self: SCollection[T]) {
  def unionInGlobalWindow(others: SCollection[T]*): SCollection[T] = {
    val commonWindowOptions = WindowOptions(
      trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    )

    self.transform { in =>
      val inputWithGlobalWindow = in.withGlobalWindow(commonWindowOptions)
      val othersWithGlobalWindow = others.map(_.withGlobalWindow(commonWindowOptions))

      SCollection.unionAll(inputWithGlobalWindow +: othersWithGlobalWindow)
    }
  }
}
private[scio] class SCollectionEitherOps[L: Coder, R: Coder](private val self: SCollection[Either[L, R]]) {
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

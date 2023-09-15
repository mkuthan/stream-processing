package org.mkuthan.streamprocessing.shared.scio.syntax

import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions

import org.mkuthan.streamprocessing.shared.scio.FixedWindowConfiguration

private[syntax] trait SCollectionSyntax {

  implicit class SCollectionOps[T: Coder](private val self: SCollection[T]) {
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

    def withFixedWindow(configuration: FixedWindowConfiguration = FixedWindowConfiguration()): SCollection[T] =
      self.withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
  }

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

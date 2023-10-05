package org.mkuthan.streamprocessing.shared.scio.syntax

import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.SumByKey

private[syntax] trait SCollectionSyntax {

  implicit class SCollectionOps[T: Coder](private val self: SCollection[T]) {
    private val GlobalWindowOptions = WindowOptions(
      trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    )

    def unionInGlobalWindow(others: SCollection[T]*): SCollection[T] =
      self.transform { in =>
        val inputWithGlobalWindow = in.withGlobalWindow(GlobalWindowOptions)
        val othersWithGlobalWindow = others.map(_.withGlobalWindow(GlobalWindowOptions))

        SCollection.unionAll(inputWithGlobalWindow +: othersWithGlobalWindow)
      }

    def mapWithTimestamp[U: Coder](mapFn: (T, Instant) => U): SCollection[U] =
      self.transform { in =>
        in
          .withTimestamp
          .map { case (element, timestamp) => mapFn(element, timestamp) }
      }
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

  implicit class SCollectionSumByKeyOps[T: Coder: SumByKey](private val self: SCollection[T]) {
    def sumByKeyInFixedWindow(
        windowDuration: Duration,
        windowOptions: WindowOptions = WindowOptions()
    ): SCollection[T] =
      self.transform { in =>
        in
          .withFixedWindows(duration = windowDuration, options = windowOptions)
          .keyBy(SumByKey[T].key)
          .sumByKey(SumByKey[T].semigroup)
          .values
      }
  }

}

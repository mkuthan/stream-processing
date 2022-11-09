package org.mkuthan.streamprocessing.toll.domain.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

// TODO: How to define dictionary for diagnostic to make it generic?
sealed trait Reason { def name: String }
case object MissingTollBoothExit extends Reason { val name = "Missing TollBoothExit to calculate TotalCarTime" }

final case class Diagnostic(
    boothId: TollBoothId,
    reason: Reason,
    counter: Int = 1
)

object Diagnostic {

  implicit val CoderCache: Coder[Diagnostic] = Coder.gen
  implicit val CoderCacheRaw: Coder[Diagnostic.Raw] = Coder.gen

  // TODO: finish type
  @BigQueryType.toTable
  final case class Raw(
      reason: String
  )

  def unionInGlobalWindow(diagnostics: SCollection[Diagnostic]*): SCollection[Diagnostic] = {
    val options = WindowOptions(
      trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    )

    val diagnosticsInGlobalWindow = diagnostics.map(_.withGlobalWindow(options))

    SCollection.unionAll(diagnosticsInGlobalWindow)
  }

  // TODO: implement
  def aggregateInFixedWindow(input: SCollection[Diagnostic], duration: Duration): SCollection[Diagnostic] =
    input.context.empty[Diagnostic]()

  // TODO: implement
  def encode(input: SCollection[Diagnostic]): SCollection[Raw] =
    input.context.empty[Raw]()
}

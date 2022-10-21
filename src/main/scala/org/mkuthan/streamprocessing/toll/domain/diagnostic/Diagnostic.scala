package org.mkuthan.streamprocessing.toll.domain.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.joda.time.Duration

final case class Diagnostic()

object Diagnostic {

  // implicit val CoderCache: Coder[Diagnostic] = Coder.gen

  @BigQueryType.toTable
  final case class Raw()

  def aggregateInFixedWindow(input: Iterable[SCollection[Diagnostic]], duration: Duration): SCollection[Diagnostic] =
    ???

  def encode(input: SCollection[Diagnostic]): SCollection[Raw] = ???
}

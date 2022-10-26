package org.mkuthan.streamprocessing.toll.domain.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import org.joda.time.Duration

final case class Diagnostic(
    reason: String,
    labels: Map[String, String],
    counter: Int = 1
)

object Diagnostic {

  // implicit val CoderCache: Coder[Diagnostic] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      reason: String
  )

  def aggregateInFixedWindow(input: SCollection[Diagnostic], duration: Duration): SCollection[Diagnostic] =
    input.context.empty[Diagnostic]()

  def encode(input: SCollection[Diagnostic]): SCollection[Raw] =
    input.context.empty[Raw]()
}

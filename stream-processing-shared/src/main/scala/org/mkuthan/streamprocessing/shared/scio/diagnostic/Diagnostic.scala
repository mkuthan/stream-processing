package org.mkuthan.streamprocessing.shared.scio.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import com.google.api.services.bigquery.model.TableRow
import com.twitter.algebird.Semigroup
import org.joda.time.Duration
import org.joda.time.Instant

case class Diagnostic(
    reason: String,
    count: Long
)

private[diagnostic] object Diagnostic {

  @BigQueryType.toTable
  case class Raw(
      createdAt: Instant,
      reason: String,
      count: Long
  )

  private val Type = BigQueryType[Raw]

  private object DiagnosticSemigroup extends Semigroup[Diagnostic] {
    override def plus(x: Diagnostic, y: Diagnostic): Diagnostic = {
      require(x.reason == y.reason)
      Diagnostic(x.reason, x.count + y.count)
    }
  }

  def aggregateInFixedWindow(
      diagnostics: SCollection[Diagnostic],
      duration: Duration,
      options: WindowOptions
  ): SCollection[Diagnostic] =
    diagnostics
      .keyBy(_.reason)
      .withFixedWindows(duration = duration, options = options)
      .sumByKey(DiagnosticSemigroup)
      .values

  def serialize(diagnostics: SCollection[Diagnostic]): SCollection[TableRow] =
    diagnostics
      .map(toRaw)
      .map(Type.toTableRow)

  private def toRaw(diagnostic: Diagnostic): Raw =
    Raw(Instant.now(), diagnostic.reason, diagnostic.count)
}

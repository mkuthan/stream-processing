package org.mkuthan.streamprocessing.infrastructure.common

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey

final case class IoDiagnostic(
    id: String,
    reason: String,
    count: Long = 1
) {
  private lazy val keyFields = this match {
    case IoDiagnostic(id, reason, count @ _) =>
      Seq(id, reason)
  }
}

object IoDiagnostic {
  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      id: String,
      reason: String,
      count: Long
  )

  implicit val sumByKey: SumByKey[IoDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def union(first: SCollection[IoDiagnostic], others: SCollection[IoDiagnostic]*): SCollection[IoDiagnostic] =
    first.unionInGlobalWindow(others: _*)

  def aggregateAndEncode(
      input: SCollection[IoDiagnostic],
      windowDuration: Duration,
      windowOptions: WindowOptions
  ): SCollection[Record] =
    input
      .sumByKeyInFixedWindow(windowDuration = windowDuration, windowOptions = windowOptions)
      .mapWithTimestamp { case (r, t) =>
        Record(
          created_at = t,
          id = r.id,
          reason = r.reason,
          count = r.count
        )
      }
}

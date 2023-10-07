package org.mkuthan.streamprocessing.shared.common

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey

// TODO: move to shared/diagnostic
final case class Diagnostic(
    id: String,
    reason: String,
    count: Long = 1
) {
  private lazy val keyFields = this match {
    case Diagnostic(id, reason, count @ _) =>
      Seq(id, reason)
  }
}

object Diagnostic {
  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      id: String,
      reason: String,
      count: Long
  )

  implicit val sumByKey: SumByKey[Diagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def aggregateAndEncodeRecord(
      input: SCollection[Diagnostic],
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

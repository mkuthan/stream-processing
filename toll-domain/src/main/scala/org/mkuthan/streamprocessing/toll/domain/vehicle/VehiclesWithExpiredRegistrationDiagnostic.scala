package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

final case class VehiclesWithExpiredRegistrationDiagnostic(
    tollBoothId: TollBoothId,
    reason: String,
    count: Long = 1L
) {
  private lazy val keyFields = this match {
    case VehiclesWithExpiredRegistrationDiagnostic(tollBoothId, reason, count @ _) =>
      Seq(tollBoothId, reason)
  }
}

object VehiclesWithExpiredRegistrationDiagnostic {

  val NotExpired = "Vehicle registration is not expired"
  val MissingRegistration = "Missing vehicle registration"

  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      toll_both_id: String,
      reason: String,
      count: Long
  )

  implicit val sumByKey: SumByKey[VehiclesWithExpiredRegistrationDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def aggregateAndEncode(
      input: SCollection[VehiclesWithExpiredRegistrationDiagnostic],
      windowDuration: Duration,
      windowOptions: WindowOptions
  ): SCollection[Record] =
    input
      .sumByKeyInFixedWindow(windowDuration = windowDuration, windowOptions = windowOptions)
      .mapWithTimestamp { case (r, t) =>
        Record(
          created_at = t,
          toll_both_id = r.tollBoothId.id,
          reason = r.reason,
          count = r.count
        )
      }
}

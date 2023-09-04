package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.SumByKey
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

case class VehiclesWithExpiredRegistrationDiagnostic(tollBothId: TollBoothId, reason: String, count: Long = 1L) {
  private lazy val keyFields = this match {
    case VehiclesWithExpiredRegistrationDiagnostic(tollBoothId, reason, count @ _) =>
      Seq(tollBoothId, reason)
  }
}

object VehiclesWithExpiredRegistrationDiagnostic {
  @BigQueryType.toTable
  case class Raw(created_at: Instant, toll_both_id: String, reason: String, count: Long = 1L)

  implicit val sumByKey: SumByKey[VehiclesWithExpiredRegistrationDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def toRaw(diagnostic: VehiclesWithExpiredRegistrationDiagnostic, createdAt: Instant): Raw =
    Raw(
      created_at = createdAt,
      toll_both_id = diagnostic.tollBothId.id,
      reason = diagnostic.reason,
      count = diagnostic.count
    )
}

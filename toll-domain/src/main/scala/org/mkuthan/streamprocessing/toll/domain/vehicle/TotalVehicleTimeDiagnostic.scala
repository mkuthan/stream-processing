package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.SumByKey
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

case class TotalVehicleTimeDiagnostic(tollBothId: TollBoothId, reason: String, count: Long = 1L) {
  private lazy val keyFields = this match {
    case TotalVehicleTimeDiagnostic(tollBoothId, reason, count @ _) =>
      Seq(tollBoothId, reason)
  }
}

object TotalVehicleTimeDiagnostic {
  @BigQueryType.toTable
  case class Raw(created_at: Instant, toll_both_id: String, reason: String, count: Long = 1L)

  implicit val sumByKey: SumByKey[TotalVehicleTimeDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def toRaw(diagnostic: TotalVehicleTimeDiagnostic, createdAt: Instant): Raw =
    Raw(
      created_at = createdAt,
      toll_both_id = diagnostic.tollBothId.id,
      reason = diagnostic.reason,
      count = diagnostic.count
    )
}

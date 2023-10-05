package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

final case class TotalVehicleTimesDiagnostic(
    tollBoothId: TollBoothId,
    reason: String,
    count: Long = 1L
) {
  private lazy val keyFields = this match {
    case TotalVehicleTimesDiagnostic(tollBoothId, reason, count @ _) =>
      Seq(tollBoothId, reason)
  }
}

object TotalVehicleTimesDiagnostic {

  val MissingTollBoothExit = "Missing TollBoothExit to calculate TotalVehicleTimes"

  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      toll_booth_id: String,
      reason: String,
      count: Long
  )

  implicit val sumByKey: SumByKey[TotalVehicleTimesDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def aggregateAndEncodeRecord(
      input: SCollection[TotalVehicleTimesDiagnostic],
      windowDuration: Duration,
      windowOptions: WindowOptions
  ): SCollection[Record] =
    input
      .sumByKeyInFixedWindow(windowDuration = windowDuration, windowOptions = windowOptions)
      .mapWithTimestamp { case (r, t) =>
        Record(
          created_at = t,
          toll_booth_id = r.tollBoothId.id,
          reason = r.reason,
          count = r.count
        )
      }
}

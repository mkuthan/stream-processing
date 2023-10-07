package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

final case class TollBoothDiagnostic(
    tollBoothId: TollBoothId,
    reason: String,
    count: Long = 1L
) {
  private lazy val keyFields = this match {
    case TollBoothDiagnostic(tollBoothId, reason, count @ _) =>
      Seq(tollBoothId, reason)
  }
}

object TollBoothDiagnostic {

  val MissingTollBoothExit = "Missing TollBoothExit to calculate TotalVehicleTimes"
  val VehicleRegistrationNotExpired = "Vehicle registration is not expired"
  val MissingVehicleRegistration = "Missing vehicle registration"

  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      toll_booth_id: String,
      reason: String,
      count: Long
  )

  implicit val sumByKey: SumByKey[TollBoothDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def aggregateAndEncodeRecord(
      input: SCollection[TollBoothDiagnostic],
      windowDuration: Duration,
      windowOptions: WindowOptions
  ): SCollection[Record] =
    input
      .sumByKeyInFixedWindow(windowDuration = windowDuration, windowOptions = windowOptions)
      .mapWithTimestamp { case (diagnostic, timestamp) =>
        Record(
          created_at = timestamp,
          toll_booth_id = diagnostic.tollBoothId.id,
          reason = diagnostic.reason,
          count = diagnostic.count
        )
      }
}

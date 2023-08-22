package org.mkuthan.streamprocessing.toll.domain.registration

import scala.util.control.NonFatal

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioMetrics

import org.apache.beam.sdk.metrics.Counter

import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class VehicleRegistration(
    id: VehicleRegistrationId,
    licensePlate: LicensePlate,
    expired: Boolean
)

object VehicleRegistration {

  type DeadLetterRaw = DeadLetter[Raw]

  val DlqCounter: Counter = ScioMetrics.counter[VehicleRegistration]("dlq")

  @BigQueryType.toTable
  final case class Raw(
      id: String,
      license_plate: String,
      expired: Int
  )

  def decode(input: SCollection[Raw]): (SCollection[VehicleRegistration], SCollection[DeadLetterRaw]) =
    input
      .map(element => fromRaw(element))
      .unzip

  def unionHistoryWithUpdates(history: SCollection[Raw], updates: SCollection[Message[Raw]]): SCollection[Raw] =
    history.unionInGlobalWindow(updates.map(_.payload))

  private def fromRaw(raw: Raw): Either[DeadLetterRaw, VehicleRegistration] =
    try {
      require(raw.expired >= 0, s"Field 'expired' must be positive but was '${raw.expired}'")
      val vehicleRegistration = VehicleRegistration(
        id = VehicleRegistrationId(raw.id),
        licensePlate = LicensePlate(raw.license_plate),
        expired = raw.expired != 0
      )
      Right(vehicleRegistration)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(raw, ex.getMessage))
    }
}

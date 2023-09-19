package org.mkuthan.streamprocessing.toll.domain.registration

import scala.util.control.NonFatal

import org.apache.beam.sdk.metrics.Counter

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioMetrics

import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate

import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class VehicleRegistration(
    id: VehicleRegistrationId,
    registrationTime: Instant,
    licensePlate: LicensePlate,
    expired: Boolean
)

object VehicleRegistration {

  type DeadLetterPayload = DeadLetter[Payload]

  val TimestampAttribute = "registration_time"

  val DlqCounter: Counter = ScioMetrics.counter[VehicleRegistration]("dlq")

  case class Payload(
      id: String,
      registration_time: String,
      license_plate: String,
      expired: String
  )

  @BigQueryType.toTable
  case class Record(
      id: String,
      license_plate: String,
      expired: Int
  )

  def decodeMessage(input: SCollection[Message[Payload]])
      : (SCollection[VehicleRegistration], SCollection[DeadLetter[Payload]]) =
    input
      .map(message => fromMessage(message))
      .unzip

  def decodeRecord(input: SCollection[Record], partitionDate: LocalDate): SCollection[VehicleRegistration] =
    input
      .map(record => fromRecord(record, partitionDate))
      .timestampBy(registration => registration.registrationTime)

  def unionHistoryWithUpdates(
      history: SCollection[VehicleRegistration],
      updates: SCollection[VehicleRegistration]
  ): SCollection[VehicleRegistration] =
    history
      .unionInGlobalWindow(updates)

  private def fromMessage(message: Message[Payload]): Either[DeadLetter[Payload], VehicleRegistration] = {
    val payload = message.payload
    try {
      val expired = payload.expired.toInt

      validateExpired(expired)

      val vehicleRegistration = VehicleRegistration(
        id = VehicleRegistrationId(payload.id),
        registrationTime = Instant.parse(payload.registration_time),
        licensePlate = LicensePlate(payload.license_plate),
        expired = expired != 0
      )
      Right(vehicleRegistration)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(payload, ex.getMessage))
    }
  }

  private def fromRecord(record: Record, partitionDate: LocalDate): VehicleRegistration = {
    validateExpired(record.expired)

    VehicleRegistration(
      id = VehicleRegistrationId(record.id),
      registrationTime = partitionDate.toDateTimeAtStartOfDay(DateTimeZone.UTC).toInstant,
      licensePlate = LicensePlate(record.license_plate),
      expired = record.expired != 0
    )
  }

  private def validateExpired(expired: Int): Unit =
    require(expired >= 0, s"Field 'expired' must be positive but was '$expired'")
}

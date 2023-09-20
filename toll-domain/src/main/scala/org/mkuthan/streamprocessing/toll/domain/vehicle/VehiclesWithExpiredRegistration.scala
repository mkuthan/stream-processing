package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

final case class VehiclesWithExpiredRegistration(
    vehicleRegistrationId: VehicleRegistrationId,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    licensePlate: LicensePlate
)

object VehiclesWithExpiredRegistration {

  val TimestampAttribute = "created_at"

  case class Payload(
      created_at: String,
      vehicle_registration_id: String,
      toll_booth_id: String,
      entry_time: String,
      license_plate: String
  )

  @BigQueryType.toTable
  case class Record(
      created_at: Instant,
      vehicle_registration_id: String,
      toll_booth_id: String,
      entry_time: Instant,
      license_plate: String
  )

  def calculateInFixedWindow(
      boothEntries: SCollection[TollBoothEntry],
      vehicleRegistrations: SCollection[VehicleRegistration],
      duration: Duration
  ): (SCollection[VehiclesWithExpiredRegistration], SCollection[VehiclesWithExpiredRegistrationDiagnostic]) = {
    val boothEntriesByLicensePlate = boothEntries
      .keyBy(_.licensePlate)
      .withFixedWindows(duration)

    val sideInputWindowOptions = WindowOptions(
      trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    )

    val vehicleRegistrationByLicensePlate = vehicleRegistrations
      .keyBy(_.licensePlate)
      .withGlobalWindow(sideInputWindowOptions)

    val results = boothEntriesByLicensePlate
      .hashLeftOuterJoin(vehicleRegistrationByLicensePlate)
      .values
      .distinct
      .map {
        case (boothEntry, Some(vehicleRegistration)) if vehicleRegistration.expired =>
          Right(toVehiclesWithExpiredRegistration(boothEntry, vehicleRegistration))
        case (boothEntry, Some(vehicleRegistration)) if !vehicleRegistration.expired =>
          val diagnosticReason = "Vehicle registration is not expired"
          Left(VehiclesWithExpiredRegistrationDiagnostic(boothEntry.id, diagnosticReason))
        case (boothEntry, None) =>
          val diagnosticReason = "Missing vehicle registration"
          Left(VehiclesWithExpiredRegistrationDiagnostic(boothEntry.id, diagnosticReason))
      }

    results.unzip
  }

  def encodeMessage(input: SCollection[VehiclesWithExpiredRegistration]): SCollection[Message[Payload]] =
    input.mapWithTimestamp { case (r, t) =>
      val payload = Payload(
        created_at = t.toString,
        vehicle_registration_id = r.vehicleRegistrationId.id,
        toll_booth_id = r.tollBoothId.id,
        entry_time = r.entryTime.toString,
        license_plate = r.licensePlate.number
      )
      val attributes = Map(TimestampAttribute -> t.toString)

      Message(payload, attributes)
    }

  def encodeRecord(input: SCollection[VehiclesWithExpiredRegistration]): SCollection[Record] =
    input.mapWithTimestamp { case (r, t) =>
      Record(
        created_at = t,
        vehicle_registration_id = r.vehicleRegistrationId.id,
        toll_booth_id = r.tollBoothId.id,
        entry_time = r.entryTime,
        license_plate = r.licensePlate.number
      )
    }

  private def toVehiclesWithExpiredRegistration(
      boothEntry: TollBoothEntry,
      vehicleRegistration: VehicleRegistration
  ): VehiclesWithExpiredRegistration =
    VehiclesWithExpiredRegistration(
      vehicleRegistrationId = vehicleRegistration.id,
      tollBoothId = boothEntry.id,
      entryTime = boothEntry.entryTime,
      licensePlate = boothEntry.licensePlate
    )
}

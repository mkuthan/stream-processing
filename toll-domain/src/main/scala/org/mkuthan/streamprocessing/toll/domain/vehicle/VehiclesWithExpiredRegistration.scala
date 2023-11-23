package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
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

  final case class Payload(
      created_at: String,
      vehicle_registration_id: String,
      toll_booth_id: String,
      entry_time: String,
      license_plate: String
  )

  @BigQueryType.toTable
  final case class Record(
      created_at: Instant,
      vehicle_registration_id: String,
      toll_booth_id: String,
      entry_time: Instant,
      license_plate: String
  )

  def calculateWithTemporalJoin(
      boothEntries: SCollection[TollBoothEntry],
      vehicleRegistrations: SCollection[VehicleRegistration],
      leftWindowDuration: Duration,
      rightWindowDuration: Duration,
      windowOptions: WindowOptions
  ): (SCollection[VehiclesWithExpiredRegistration], SCollection[TollBoothDiagnostic]) = {
    val results = boothEntries.transform { in =>
      val boothEntriesByLicensePlate = in
        .keyBy(_.licensePlate)
        .withFixedWindows(
          duration = leftWindowDuration,
          options = windowOptions
        )

      val vehicleRegistrationByLicensePlate = vehicleRegistrations
        .keyBy(_.licensePlate)
        .withFixedWindows(
          duration = rightWindowDuration,
          options = windowOptions
        )

      boothEntriesByLicensePlate
        .hashLeftOuterJoin(vehicleRegistrationByLicensePlate)
        .values
        .map {
          case (boothEntry, Some(vehicleRegistration)) if vehicleRegistration.expired =>
            Right(toVehiclesWithExpiredRegistration(boothEntry, vehicleRegistration))
          case (boothEntry, Some(vehicleRegistration)) if !vehicleRegistration.expired =>
            Left(TollBoothDiagnostic(
              boothEntry.id,
              TollBoothDiagnostic.VehicleRegistrationNotExpired
            ))
          case (boothEntry, None) =>
            Left(TollBoothDiagnostic(
              boothEntry.id,
              TollBoothDiagnostic.MissingVehicleRegistration
            ))
        }
    }

    results.partition()
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

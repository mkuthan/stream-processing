package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import org.joda.time.Instant
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

final case class VehiclesWithExpiredRegistration(
    licensePlate: LicensePlate,
    tollBoothId: TollBoothId,
    vehicleRegistrationId: VehicleRegistrationId,
    entryTime: Instant
)

object VehiclesWithExpiredRegistration {

  @BigQueryType.toTable
  final case class Raw(
      license_plate: String,
      toll_both_id: String,
      vehicle_registration_id: String,
      entryTime: Instant
  )

  def calculate(
      boothEntries: SCollection[TollBoothEntry],
      vehicleRegistration: SCollection[VehicleRegistration]
  ): (SCollection[VehiclesWithExpiredRegistration], SCollection[Diagnostic]) = ???

  def encode(input: SCollection[VehiclesWithExpiredRegistration]): SCollection[Raw] = ???
}

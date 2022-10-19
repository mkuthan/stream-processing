package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.bigquery.types.BigQueryTag
import org.joda.time.Instant
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

@BigQuery
case class VehiclesWithExpiredRegistration(
    licensePlate: LicensePlate,
    tollId: TollBoothId,
    vehicleRegistrationId: VehicleRegistrationId,
    entryTime: Instant
)

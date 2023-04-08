package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.bigquery.BigQueryType

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStatsFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTimeFixture
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes

trait TollApplicationFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothEntryStatsFixture
    with TotalCarTimeFixture
    with VehicleRegistrationFixture {

  val anyTollBoothEntryRawJson = writeJsonAsBytes(anyTollBoothEntryRaw)
  val tollBoothEntryRawInvalidJson = writeJsonAsBytes(tollBoothEntryRawInvalid)

  val anyTollBoothExitRawJson = writeJsonAsBytes(anyTollBoothExitRaw)
  val tollBoothExitRawInvalidJson = writeJsonAsBytes(tollBoothExitRawInvalid)

  val anyTollBoothEntryStatsRawTableRow = BigQueryType[TollBoothEntryStats.Raw].toTableRow(anyTollBoothEntryStatsRaw)
  val anyTotalCarTimeRawTableRow = BigQueryType[TotalCarTime.Raw].toTableRow(anyTotalCarTimeRaw)

  val anyVehicleRegistrationRawTableRow = BigQueryType[VehicleRegistration.Raw].toTableRow(anyVehicleRegistrationRaw)
}

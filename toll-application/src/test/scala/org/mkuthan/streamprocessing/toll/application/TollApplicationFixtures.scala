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
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

trait TollApplicationFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothEntryStatsFixture
    with TotalCarTimeFixture
    with VehicleRegistrationFixture {

  val anyTollBoothEntryRawJson = JsonSerde.writeJsonAsBytes(anyTollBoothEntryRaw)
  val tollBoothEntryRawInvalidJson = JsonSerde.writeJsonAsBytes(tollBoothEntryRawInvalid)

  val anyTollBoothExitRawJson = JsonSerde.writeJsonAsBytes(anyTollBoothExitRaw)
  val tollBoothExitRawInvalidJson = JsonSerde.writeJsonAsBytes(tollBoothExitRawInvalid)

  val anyTollBoothEntryStatsRawTableRow = BigQueryType[TollBoothEntryStats.Raw].toTableRow(anyTollBoothEntryStatsRaw)
  val anyTotalCarTimeRawTableRow = BigQueryType[TotalCarTime.Raw].toTableRow(anyTotalCarTimeRaw)

  val anyVehicleRegistrationRawTableRow = BigQueryType[VehicleRegistration.Raw].toTableRow(anyVehicleRegistrationRaw)

  val tollBoothEntryDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothEntryDecodingError)
  val tollBoothExitDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothExitDecodingError)
}

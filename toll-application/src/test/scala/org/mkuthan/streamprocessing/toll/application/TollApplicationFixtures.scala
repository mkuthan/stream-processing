package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.bigquery.BigQueryType

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStatsFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTimeFixture

trait TollApplicationFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothEntryStatsFixture
    with TotalCarTimeFixture
    with VehicleRegistrationFixture {

  val corruptedJsonPubsubMessage = new PubsubMessage("corrupted".getBytes, null)

  val tollBoothEntryTime = anyTollBoothEntry.entryTime
  val tollBoothEntryPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(anyTollBoothEntryRaw), null)
  val invalidTollBoothEntryPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(tollBoothEntryRawInvalid), null)

  val tollBoothExitTime = anyTollBoothExit.exitTime
  val tollBoothExitPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(anyTollBoothExitRaw), null)
  val invalidTollBoothExitPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(tollBoothExitRawInvalid), null)

  val anyTollBoothEntryStatsRawTableRow = BigQueryType[TollBoothEntryStats.Raw].toTableRow(anyTollBoothEntryStatsRaw)
  val anyTotalCarTimeRawTableRow = BigQueryType[TotalCarTime.Raw].toTableRow(anyTotalCarTimeRaw)

  val anyVehicleRegistrationRawPubsubMessage =
    new PubsubMessage(JsonSerde.writeJsonAsBytes(anyVehicleRegistrationRaw), null)
  val anyVehicleRegistrationRawTableRow = BigQueryType[VehicleRegistration.Raw].toTableRow(anyVehicleRegistrationRaw)

  val tollBoothEntryDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothEntryDecodingError)
  val tollBoothExitDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothExitDecodingError)
}

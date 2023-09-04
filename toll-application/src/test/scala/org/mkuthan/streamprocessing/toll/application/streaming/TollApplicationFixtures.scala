package org.mkuthan.streamprocessing.toll.application.streaming

import java.{util => ju}

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

import com.spotify.scio.bigquery.BigQueryType

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStatsFixture
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationFixture

trait TollApplicationFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothStatsFixture
    with TotalVehicleTimeFixture
    with VehicleRegistrationFixture
    with VehiclesWithExpiredRegistrationFixture {

  val emptyMap = ju.Map.of[String, String]

  val corruptedJsonPubsubMessage = new PubsubMessage("corrupted".getBytes, emptyMap)

  val tollBoothEntryTime = anyTollBoothEntry.entryTime
  val tollBoothEntryPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(anyTollBoothEntryRaw), emptyMap)
  val invalidTollBoothEntryPubsubMessage =
    new PubsubMessage(JsonSerde.writeJsonAsBytes(tollBoothEntryRawInvalid), emptyMap)

  val tollBoothExitTime = anyTollBoothExit.exitTime
  val tollBoothExitPubsubMessage = new PubsubMessage(JsonSerde.writeJsonAsBytes(anyTollBoothExitRaw), emptyMap)
  val invalidTollBoothExitPubsubMessage =
    new PubsubMessage(JsonSerde.writeJsonAsBytes(tollBoothExitRawInvalid), emptyMap)

  val anyTollBoothStatsRawTableRow = BigQueryType[TollBoothStats.Raw].toTableRow(anyTollBoothStatsRaw)
  val anyTotalVehicleTimeRawTableRow = BigQueryType[TotalVehicleTime.Raw].toTableRow(anyTotalVehicleTimeRaw)

  val anyVehicleRegistrationRawPubsubMessage =
    new PubsubMessage(JsonSerde.writeJsonAsBytes(anyVehicleRegistrationRaw), emptyMap)
  val anyVehicleRegistrationRawTableRow = BigQueryType[VehicleRegistration.Raw].toTableRow(anyVehicleRegistrationRaw)

  val anyVehicleWithExpiredRegistrationRawPubsubMessage =
    new PubsubMessage(JsonSerde.writeJsonAsBytes(anyVehicleWithExpiredRegistrationRaw), emptyMap)

  val tollBoothEntryDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothEntryDecodingError)
  val tollBoothExitDecodingErrorString = JsonSerde.writeJsonAsString(tollBoothExitDecodingError)
}

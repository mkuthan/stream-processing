package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.shared.configuration.BigQueryTable
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageLocation

final case class TollApplicationConfig(
    entrySubscription: PubSubSubscription[TollBoothEntry.Raw],
    entryDlq: StorageLocation[TollBoothEntry.Raw],
    exitSubscription: PubSubSubscription[TollBoothExit.Raw],
    exitDlq: StorageLocation[TollBoothExit.Raw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageLocation[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothEntryStats.Raw],
    carTotalTimeTable: BigQueryTable[TotalCarTime.Raw],
    vehiclesWithExpiredRegistrationTopic: PubSubTopic[VehiclesWithExpiredRegistration.Raw],
    diagnosticTable: BigQueryTable[Diagnostic.Raw]
)

object TollApplicationConfig {
  def parse(args: Args): TollApplicationConfig = TollApplicationConfig(
    entrySubscription = PubSubSubscription(args.required("entrySubscription")),
    entryDlq = StorageLocation(args.required("entryDlq")),
    exitSubscription = PubSubSubscription(args.required("exitSubscription")),
    exitDlq = StorageLocation(args.required("exitDlq")),
    vehicleRegistrationTable = BigQueryTable.fromString(args.required("vehicleRegistrationTable")),
    vehicleRegistrationDlq = StorageLocation(args.required("vehicleRegistrationDlq")),
    entryStatsTable = BigQueryTable.fromString(args.required("entryCountTable")),
    carTotalTimeTable = BigQueryTable.fromString(args.required("carTotalTimeTable")),
    vehiclesWithExpiredRegistrationTopic = PubSubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    diagnosticTable = BigQueryTable.fromString(args.required("diagnosticTable"))
  )
}

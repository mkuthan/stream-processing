package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.shared.configuration.BigQueryTable
import org.mkuthan.streamprocessing.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic
import org.mkuthan.streamprocessing.shared.configuration.StorageBucket
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration

case class TollApplicationConfig(
    entrySubscription: PubSubSubscription[TollBoothEntry.Raw],
    entryDlq: StorageBucket[TollBoothEntry.DeadLetterRaw],
    exitSubscription: PubSubSubscription[TollBoothExit.Raw],
    exitDlq: StorageBucket[TollBoothExit.DeadLetterRaw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothEntryStats.Raw],
    carTotalTimeTable: BigQueryTable[TotalCarTime.Raw],
    vehiclesWithExpiredRegistrationTopic: PubSubTopic[VehiclesWithExpiredRegistration.Raw],
    diagnosticTable: BigQueryTable[Diagnostic.Raw]
)

object TollApplicationConfig {
  def parse(args: Args): TollApplicationConfig = TollApplicationConfig(
    entrySubscription = PubSubSubscription(args.required("entrySubscription")),
    entryDlq = StorageBucket(args.required("entryDlq")),
    exitSubscription = PubSubSubscription(args.required("exitSubscription")),
    exitDlq = StorageBucket(args.required("exitDlq")),
    vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
    vehicleRegistrationDlq = StorageBucket(args.required("vehicleRegistrationDlq")),
    entryStatsTable = BigQueryTable(args.required("entryStatsTable")),
    carTotalTimeTable = BigQueryTable(args.required("carTotalTimeTable")),
    vehiclesWithExpiredRegistrationTopic = PubSubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    diagnosticTable = BigQueryTable(args.required("diagnosticTable"))
  )
}

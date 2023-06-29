package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.PubsubSubscription
import org.mkuthan.streamprocessing.shared.scio.common.PubsubTopic
import org.mkuthan.streamprocessing.shared.scio.common.StorageBucket
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration

case class TollApplicationConfig(
    entrySubscription: PubsubSubscription[TollBoothEntry.Raw],
    entryDlq: StorageBucket[TollBoothEntry.DeadLetterRaw],
    exitSubscription: PubsubSubscription[TollBoothExit.Raw],
    exitDlq: StorageBucket[TollBoothExit.DeadLetterRaw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothEntryStats.Raw],
    carTotalTimeTable: BigQueryTable[TotalCarTime.Raw],
    vehiclesWithExpiredRegistrationTopic: PubsubTopic[VehiclesWithExpiredRegistration.Raw],
    diagnosticTable: BigQueryTable[Diagnostic.Raw]
)

object TollApplicationConfig {
  def parse(args: Args): TollApplicationConfig = TollApplicationConfig(
    entrySubscription = PubsubSubscription(args.required("entrySubscription")),
    entryDlq = StorageBucket(args.required("entryDlq")),
    exitSubscription = PubsubSubscription(args.required("exitSubscription")),
    exitDlq = StorageBucket(args.required("exitDlq")),
    vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
    vehicleRegistrationDlq = StorageBucket(args.required("vehicleRegistrationDlq")),
    entryStatsTable = BigQueryTable(args.required("entryStatsTable")),
    carTotalTimeTable = BigQueryTable(args.required("carTotalTimeTable")),
    vehiclesWithExpiredRegistrationTopic = PubsubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    diagnosticTable = BigQueryTable(args.required("diagnosticTable"))
  )
}

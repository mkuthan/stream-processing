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
import org.mkuthan.streamprocessing.toll.shared.configuration.StorageBucket

final case class TollApplicationConfig(
    entrySubscription: PubSubSubscription[TollBoothEntry.Raw],
    entryDlq: StorageBucket[TollBoothEntry.Raw],
    exitSubscription: PubSubSubscription[TollBoothExit.Raw],
    exitDlq: StorageBucket[TollBoothExit.Raw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    vehicleRegistrationDlq: StorageBucket[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothEntryStats.Raw],
    carTotalTimeTable: BigQueryTable[TotalCarTime.Raw],
    vehiclesWithExpiredRegistrationTopic: PubSubTopic[VehiclesWithExpiredRegistration.Raw],
    diagnosticTable: BigQueryTable[Diagnostic.Raw]
)

object TollApplicationConfig {
  def parse(args: Args): TollApplicationConfig = TollApplicationConfig(
    entrySubscription = PubSubSubscription(
      subscription = args.required("entrySubscription"),
      idAttribute = None,
      tsAttribute = None
    ),
    entryDlq = StorageBucket(
      bucket = args.required("entryDlq"),
      numShards = 1
    ),
    exitSubscription = PubSubSubscription(
      subscription = args.required("exitSubscription"),
      idAttribute = None,
      tsAttribute = None
    ),
    exitDlq = StorageBucket(
      bucket = args.required("exitDlq"),
      numShards = 1
    ),
    vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
    vehicleRegistrationDlq = StorageBucket(
      bucket = args.required("vehicleRegistrationDlq"),
      numShards = 1
    ),
    entryStatsTable = BigQueryTable(args.required("entryStatsTable")),
    carTotalTimeTable = BigQueryTable(args.required("carTotalTimeTable")),
    vehiclesWithExpiredRegistrationTopic = PubSubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    diagnosticTable = BigQueryTable(args.required("diagnosticTable"))
  )
}

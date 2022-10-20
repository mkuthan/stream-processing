package org.mkuthan.streamprocessing.toll.configuration

import com.spotify.scio.Args

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.dlq.DeadLetterQueue
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration

final case class TollApplicationConfiguration(
    entrySubscription: PubSubSubscription[TollBoothEntry.Raw],
    exitSubscription: PubSubSubscription[TollBoothExit.Raw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistration.Raw],
    entryStatsTable: BigQueryTable[TollBoothEntryStats.Raw],
    carTotalTimeTable: BigQueryTable[TotalCarTime.Raw],
    diagnosticTable: BigQueryTable[Diagnostic.Raw],
    vehiclesWithExpiredRegistrationTopic: PubSubTopic[VehiclesWithExpiredRegistration.Raw],
    dlqBucket: StorageBucket[DeadLetterQueue.Raw]
)

object TollApplicationConfiguration {
  def parse(args: Args): TollApplicationConfiguration = TollApplicationConfiguration(
    entrySubscription = PubSubSubscription(args.required("entrySubscription")),
    exitSubscription = PubSubSubscription(args.required("exitSubscription")),
    vehicleRegistrationTable = BigQueryTable(args.required("vehicleRegistrationTable")),
    entryStatsTable = BigQueryTable(args.required("entryCountTable")),
    carTotalTimeTable = BigQueryTable(args.required("carTotalTimeTable")),
    diagnosticTable = BigQueryTable(args.required("diagnosticTable")),
    vehiclesWithExpiredRegistrationTopic = PubSubTopic(args.required("vehiclesWithExpiredRegistrationTopic")),
    dlqBucket = StorageBucket(args.required("dlqBucket"))
  )
}

package org.mkuthan.streamprocessing.toll.configuration

import com.spotify.scio.Args
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryRaw
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStatsRaw
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitRaw
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.dlq.DeadLetterQueue
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationRaw
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.VehiclesWithExpiredRegistration

case class TollApplicationConfiguration(
    entrySubscription: PubSubSubscription[TollBoothEntryRaw],
    exitSubscription: PubSubSubscription[TollBoothExitRaw],
    vehicleRegistrationTable: BigQueryTable[VehicleRegistrationRaw],
    entryStatsTable: BigQueryTable[TollBoothEntryStatsRaw],
    carTotalTimeTable: BigQueryTable[TotalCarTime],
    diagnosticTable: BigQueryTable[Diagnostic],
    vehiclesWithExpiredRegistrationTopic: PubSubTopic[VehiclesWithExpiredRegistration],
    dlqBucket: StorageBucket[DeadLetterQueue]
)

object TollApplicationConfiguration {
  def apply(args: Args): TollApplicationConfiguration = TollApplicationConfiguration(
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

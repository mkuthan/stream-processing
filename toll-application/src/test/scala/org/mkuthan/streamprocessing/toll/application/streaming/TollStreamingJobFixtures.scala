package org.mkuthan.streamprocessing.toll.application.streaming

import org.joda.time.Instant

import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration

trait TollStreamingJobFixtures {

  final val tollBoothEntryPubsubDeadLetter: PubsubDeadLetter[TollBoothEntry.Payload] =
    PubsubDeadLetter("any entry payload".getBytes, Map(), "any entry error")

  final val tollBoothExitPubsubDeadLetter: PubsubDeadLetter[TollBoothExit.Payload] =
    PubsubDeadLetter("any exit payload".getBytes, Map(), "any exit error")

  final val vehicleRegistrationPubsubDeadLetter: PubsubDeadLetter[VehicleRegistration.Payload] =
    PubsubDeadLetter("any vehicle registration".getBytes, Map(), "any vehicle registration error")

  final val anyIoDiagnosticRecord: Diagnostic.Record = Diagnostic.Record(
    created_at = Instant.EPOCH,
    id = "any id",
    reason = "any reason",
    count = 1
  )
}

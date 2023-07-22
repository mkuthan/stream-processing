package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.shared.scio.core.Counter
import org.mkuthan.streamprocessing.shared.scio.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration

trait TollApplicationMetrics {
  val TollBoothEntryRawInvalidRows: Counter[PubsubDeadLetter[TollBoothEntry.Raw]] =
    Counter[PubsubDeadLetter[TollBoothEntry.Raw]]("TollBoothEntry", "InvalidRows")
  val TollBoothExitRawInvalidRows: Counter[PubsubDeadLetter[TollBoothExit.Raw]] =
    Counter[PubsubDeadLetter[TollBoothExit.Raw]]("TollBoothExit", "InvalidRows")
  val VehicleRegistrationRawInvalidRows: Counter[PubsubDeadLetter[VehicleRegistration.Raw]] =
    Counter[PubsubDeadLetter[VehicleRegistration.Raw]]("VehicleRegistration", "InvalidRows")
}

package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.shared.scio.core.Counter
import org.mkuthan.streamprocessing.shared.scio.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit

trait TollApplicationMetrics {
  val TollBoothEntryRawInvalidRows: Counter[PubsubDeadLetter[TollBoothEntry.Raw]] =
    Counter[PubsubDeadLetter[TollBoothEntry.Raw]]("TollBoothEntry", "InvalidRows")
  val TollBoothExitRawInvalidRows: Counter[PubsubDeadLetter[TollBoothExit.Raw]] =
    Counter[PubsubDeadLetter[TollBoothExit.Raw]]("TollBoothExit", "InvalidRows")
}

package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.infrastructure.scio.core.Counter
import org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub.PubsubDeadLetter

trait TollApplicationMetrics {
  val TollBoothEntryRawInvalidRows = Counter[PubsubDeadLetter[TollBoothEntry.Raw]]("TollBoothEntry", "InvalidRows")
  val TollBoothExitRawInvalidRows = Counter[PubsubDeadLetter[TollBoothExit.Raw]]("TollBoothExit", "InvalidRows")
}

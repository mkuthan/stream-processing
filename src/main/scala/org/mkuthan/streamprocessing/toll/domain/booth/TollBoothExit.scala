package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.values.SCollection
import org.joda.time.Instant
import org.mkuthan.streamprocessing.toll.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class TollBoothExitRaw(
    id: String,
    exitTime: String,
    licensePlate: String
)

case class TollBoothExit(
    id: TollBoothId,
    exitTime: Instant,
    licensePlate: LicensePlate
)

object TollBoothExit {
  def subscribe(subscription: PubSubSubscription[String]): SCollection[TollBoothExitRaw] = ???
  def decode(raw: SCollection[TollBoothExitRaw]): (SCollection[TollBoothExit], SCollection[TollBoothExitRaw]) = ???
}

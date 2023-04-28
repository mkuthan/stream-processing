package org.mkuthan.streamprocessing.toll.domain.booth

import scala.util.control.NonFatal

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioMetrics

import org.apache.beam.sdk.metrics.Counter
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.pubsub.PubsubMessage
import org.mkuthan.streamprocessing.toll.domain.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licensePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {

  type DeadLetterRaw = DeadLetter[Raw]

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothEntry]("dlq")

  final case class Raw(
      id: String,
      entry_time: String,
      license_plate: String,
      state: String,
      make: String,
      model: String,
      vehicle_type: String,
      weight_type: String,
      toll: String,
      tag: String
  )

  def decode(input: SCollection[PubsubMessage[Raw]]): (SCollection[TollBoothEntry], SCollection[DeadLetterRaw]) =
    input
      .map(element => fromRaw(element.payload))
      .unzip

  private def fromRaw(raw: Raw): Either[DeadLetterRaw, TollBoothEntry] =
    try {
      val tollBoothEntry = TollBoothEntry(
        id = TollBoothId(raw.id),
        entryTime = Instant.parse(raw.entry_time),
        licensePlate = LicensePlate(raw.license_plate),
        toll = BigDecimal(raw.toll)
      )
      Right(tollBoothEntry)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(raw, ex.getMessage))
    }
}

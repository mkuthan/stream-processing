package org.mkuthan.streamprocessing.toll.domain.booth

import scala.util.control.NonFatal

import org.apache.beam.sdk.metrics.Counter

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioMetrics

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licensePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {

  type DeadLetterPayload = DeadLetter[Payload]

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothEntry]("dlq")

  case class Payload(
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

  @BigQueryType.toTable
  case class Record(
      id: String,
      entry_time: Instant,
      license_plate: String,
      state: String,
      make: String,
      model: String,
      vehicle_type: String,
      weight_type: String,
      toll: String,
      tag: String
  )

  def decodePayload(input: SCollection[Message[Payload]])
      : (SCollection[TollBoothEntry], SCollection[DeadLetterPayload]) =
    input
      .map(element => fromPayload(element.payload))
      .unzip

  private def fromPayload(raw: Payload): Either[DeadLetterPayload, TollBoothEntry] =
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

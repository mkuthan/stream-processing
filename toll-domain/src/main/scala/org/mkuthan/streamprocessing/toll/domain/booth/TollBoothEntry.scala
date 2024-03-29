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

final case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licensePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {

  type DeadLetterPayload = DeadLetter[Payload]

  val TimestampAttribute = "entry_time"
  val PartitioningColumnName = "entry_time"

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothEntry]("dlq")

  final case class Payload(
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
  final case class Record(
      id: String,
      entry_time: Instant,
      license_plate: String,
      state: String,
      make: String,
      model: String,
      vehicle_type: String,
      weight_type: String,
      toll: BigDecimal,
      tag: String
  )

  def decodeMessage(
      input: SCollection[Message[Payload]]
  ): (SCollection[TollBoothEntry], SCollection[DeadLetterPayload]) =
    input
      .map(message => fromMessage(message))
      .partition()

  def decodeRecord(input: SCollection[Record]): SCollection[TollBoothEntry] =
    input.transform { in =>
      in
        .map(record => fromRecord(record))
        .timestampBy(boothEntry => boothEntry.entryTime)
    }

  private def fromMessage(message: Message[Payload]): Either[DeadLetterPayload, TollBoothEntry] = {
    val payload = message.payload
    try {
      val tollBoothEntry = TollBoothEntry(
        id = TollBoothId(payload.id),
        entryTime = Instant.parse(payload.entry_time),
        licensePlate = LicensePlate(payload.license_plate),
        toll = BigDecimal(payload.toll)
      )
      Right(tollBoothEntry)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(payload, ex.getMessage))
    }
  }

  private def fromRecord(record: Record): TollBoothEntry =
    TollBoothEntry(
      id = TollBoothId(record.id),
      entryTime = record.entry_time,
      licensePlate = LicensePlate(record.license_plate),
      toll = record.toll
    )
}

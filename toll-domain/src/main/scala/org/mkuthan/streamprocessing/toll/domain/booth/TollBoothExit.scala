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

final case class TollBoothExit(
    id: TollBoothId,
    exitTime: Instant,
    licensePlate: LicensePlate
)

object TollBoothExit {

  type DeadLetterPayload = DeadLetter[Payload]

  val TimestampAttribute = "exit_time"
  val PartitioningColumnName = "exit_time"

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothExit]("dlq")

  final case class Payload(
      id: String,
      exit_time: String,
      license_plate: String
  )

  @BigQueryType.toTable
  final case class Record(
      id: String,
      exit_time: Instant,
      license_plate: String
  )

  def decodeMessage(
      input: SCollection[Message[Payload]]
  ): (SCollection[TollBoothExit], SCollection[DeadLetterPayload]) =
    input
      .map(message => fromMessage(message))
      .partition()

  def decodeRecord(input: SCollection[Record]): SCollection[TollBoothExit] =
    input.transform { in =>
      in
        .map(record => fromRecord(record))
        .timestampBy(boothExit => boothExit.exitTime)
    }

  private def fromMessage(message: Message[Payload]): Either[DeadLetterPayload, TollBoothExit] = {
    val payload = message.payload
    try {
      val tollBoothExit = TollBoothExit(
        id = TollBoothId(payload.id),
        exitTime = Instant.parse(payload.exit_time),
        licensePlate = LicensePlate(payload.license_plate)
      )
      Right(tollBoothExit)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(payload, ex.getMessage))
    }
  }

  private def fromRecord(record: Record): TollBoothExit =
    TollBoothExit(
      id = TollBoothId(record.id),
      exitTime = record.exit_time,
      licensePlate = LicensePlate(record.license_plate)
    )
}

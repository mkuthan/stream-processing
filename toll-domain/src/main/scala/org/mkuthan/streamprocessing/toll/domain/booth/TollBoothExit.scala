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

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothExit]("dlq")

  case class Payload(
      id: String,
      exit_time: String,
      license_plate: String
  )

  @BigQueryType.toTable
  case class Record(
      id: String,
      exit_time: Instant,
      license_plate: String
  )

  def decodePayload(input: SCollection[Message[Payload]])
      : (SCollection[TollBoothExit], SCollection[DeadLetterPayload]) =
    input
      .map(element => fromPayload(element.payload))
      .unzip

  private def fromPayload(raw: Payload): Either[DeadLetterPayload, TollBoothExit] =
    try {
      val tollBoothExit = TollBoothExit(
        id = TollBoothId(raw.id),
        exitTime = Instant.parse(raw.exit_time),
        licensePlate = LicensePlate(raw.license_plate)
      )
      Right(tollBoothExit)
    } catch {
      case NonFatal(ex) =>
        DlqCounter.inc()
        Left(DeadLetter(raw, ex.getMessage))
    }
}

package org.mkuthan.streamprocessing.toll.domain.booth

import scala.util.control.NonFatal

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioMetrics

import org.apache.beam.sdk.metrics.Counter
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TollBoothExit(
    id: TollBoothId,
    exitTime: Instant,
    licensePlate: LicensePlate
)

object TollBoothExit {

  type DeadLetterRaw = DeadLetter[Raw]

  val DlqCounter: Counter = ScioMetrics.counter[TollBoothExit]("dlq")

  final case class Raw(
      id: String,
      exit_time: String,
      license_plate: String
  )

  def decode(input: SCollection[Message[Raw]]): (SCollection[TollBoothExit], SCollection[DeadLetterRaw]) =
    input
      .map(element => fromRaw(element.payload))
      .unzip

  private def fromRaw(raw: Raw): Either[DeadLetterRaw, TollBoothExit] =
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

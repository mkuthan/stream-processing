package org.mkuthan.streamprocessing.toll.domain.booth

import scala.util.control.NonFatal
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.ScioMetrics
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.metrics.Counter
import org.joda.time.Instant
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licensePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {

  implicit val CoderCache: Coder[TollBoothEntry] = Coder.gen
  implicit val CoderCacheRaw: Coder[TollBoothEntry.Raw] = Coder.gen

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

  def decode(inputs: SCollection[Raw]): (SCollection[TollBoothEntry], SCollection[Raw]) = {
    val dlq = SideOutput[Raw]()
    val (results, sideOutputs) = inputs
      .withSideOutputs(dlq)
      .flatMap { case (input, ctx) =>
        try
          Some(fromRaw(input))
        catch {
          case NonFatal(_) =>
            ctx.output(dlq, input)
            DlqCounter.inc()
            None
        }
      }

    (results, sideOutputs(dlq))
  }

  private def fromRaw(raw: Raw): TollBoothEntry =
    TollBoothEntry(
      id = TollBoothId(raw.id),
      entryTime = Instant.parse(raw.entry_time),
      licensePlate = LicensePlate(raw.license_plate),
      toll = BigDecimal(raw.toll)
    )
}

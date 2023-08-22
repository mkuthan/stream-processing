package org.mkuthan.streamprocessing.toll.domain.registration

import scala.util.control.NonFatal

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.ScioMetrics

import org.apache.beam.sdk.metrics.Counter

import org.mkuthan.streamprocessing.shared._
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class VehicleRegistration(
    id: VehicleRegistrationId,
    licensePlate: LicensePlate,
    expired: Boolean
)

object VehicleRegistration {

  val DlqCounter: Counter = ScioMetrics.counter[VehicleRegistration]("dlq")

  @BigQueryType.toTable
  final case class Raw(
      id: String,
      license_plate: String,
      expired: Int
  )

  // TODO: test
  def unionHistoryWithUpdates(history: SCollection[Raw], updates: SCollection[Message[Raw]]): SCollection[Raw] =
    history.unionInGlobalWindow(updates.map(_.payload))

  def decode(inputs: SCollection[Raw]): (SCollection[VehicleRegistration], SCollection[Raw]) = {
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

  private def fromRaw(raw: Raw): VehicleRegistration = {
    require(raw.expired >= 0)
    VehicleRegistration(
      id = VehicleRegistrationId(raw.id),
      licensePlate = LicensePlate(raw.license_plate),
      expired = raw.expired != 0
    )
  }
}

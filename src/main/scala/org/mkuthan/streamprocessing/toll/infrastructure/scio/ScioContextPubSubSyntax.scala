package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions
import scala.util.Try

import com.spotify.scio.coders.Coder
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  def subscribeToPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T]
  ): SCollection[T] =
    self
      .read(PubsubIO.string(subscription.id))(PubsubIO.ReadParam(PubsubIO.Subscription))
      .map(JsonSerde.read[T])
}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
trait ScioContextPubSubSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

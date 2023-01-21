package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJson
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  def subscribeJsonFromPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T]
  ): SCollection[T] = {
    val io = PubsubIO
      .readStrings()
      .fromSubscription(subscription.id)

    subscription.idAttribute.foreach(io.withIdAttribute(_))
    subscription.tsAttribute.foreach(io.withTimestampAttribute(_))

    self
      .customInput(subscription.id, io)
      .map(readJson[T])
  }
}

trait ScioContextPubSubSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

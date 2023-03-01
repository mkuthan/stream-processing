package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  def subscribeJsonFromPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T],
      idAttribute: Option[String] = None,
      tsAttribute: Option[String] = None
  ): SCollection[PubSubMessage[T]] = {
    import scala.jdk.CollectionConverters._

    val io = PubsubIO
      .readMessagesWithAttributes()
      .fromSubscription(subscription.id)

    idAttribute.foreach(io.withIdAttribute(_))
    tsAttribute.foreach(io.withTimestampAttribute(_))

    self
      .customInput(subscription.id, io)
      .map { msg =>
        val payload = readJsonFromBytes(msg.getPayload()).get // TODO: handle errors
        val attributes = if (msg.getAttributeMap() == null) {
          Map.empty[String, String]
        } else {
          msg.getAttributeMap().asScala.toMap
        }
        PubSubMessage(payload, attributes)
      }
  }
}

trait ScioContextPubSubSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

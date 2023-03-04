package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Failure
import scala.util.Success

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  import PubSubScioContextOps._

  def subscribeJsonFromPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T],
      idAttribute: Option[String] = None,
      tsAttribute: Option[String] = None
  ): (SCollection[PubSubMessage[T]], SCollection[PubSubDeserializationError[T]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .fromSubscription(subscription.id)

    idAttribute.foreach(io.withIdAttribute(_))
    tsAttribute.foreach(io.withTimestampAttribute(_))

    val dlq = SideOutput[PubSubDeserializationError[T]]()

    val (results, sideOutputs) = self
      .customInput(subscription.id, io)
      .withSideOutputs(dlq)
      .flatMap { (msg, ctx) =>
        val payload = msg.getPayload()
        val attributes = readAttributes(msg.getAttributeMap)

        readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Some(PubSubMessage(deserialized, attributes))
          case Failure(ex) =>
            ctx.output(dlq, PubSubDeserializationError[T](payload, attributes, ex))
            None
        }
      }

    (results, sideOutputs(dlq))
  }
}

object PubSubScioContextOps {
  private def readAttributes(attributes: JMap[String, String]): Map[String, String] =
    if (attributes == null) {
      Map.empty[String, String]
    } else {
      attributes.asScala.toMap
    }
}

trait ScioContextPubSubSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

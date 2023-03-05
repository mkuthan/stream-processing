package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
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

    val messagesOrDeserializationErrors = self
      .customInput(subscription.id, io)
      .map { msg =>
        val payload = msg.getPayload()
        val attributes = readAttributes(msg.getAttributeMap)

        readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Right(PubSubMessage(deserialized, attributes))
          case Failure(ex) =>
            Left(PubSubDeserializationError[T](payload, attributes, ex.getMessage))
        }
      }

    val deserializationErrorOutput = SideOutput[PubSubDeserializationError[T]]()

    val (messages, sideOutputs) = messagesOrDeserializationErrors
      .withSideOutputs(deserializationErrorOutput)
      .flatMap {
        case (Right(message), _) => Some(message)
        case (Left(deserializationError), ctx) =>
          ctx.output(deserializationErrorOutput, deserializationError)
          None
      }

    (messages, sideOutputs(deserializationErrorOutput))
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
  import scala.language.implicitConversions

  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

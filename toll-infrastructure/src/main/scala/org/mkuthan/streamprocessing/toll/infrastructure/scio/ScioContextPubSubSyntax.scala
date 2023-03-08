package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.Failure
import scala.util.Success

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.infrastructure.scio.PubSubDeadLetter
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.shared.core.SCollectionSyntax._

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  import PubSubScioContextOps._

  def subscribeJsonFromPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T],
      idAttribute: Option[PubSubAttribute.Id] = None,
      tsAttribute: Option[PubSubAttribute.Timestamp] = None
  ): (SCollection[PubSubMessage[T]], SCollection[PubSubDeadLetter[T]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .pipe(r => idAttribute.map(_.configure(r)).getOrElse(r))
      .pipe(r => tsAttribute.map(_.configure(r)).getOrElse(r))
      .fromSubscription(subscription.id)

    val messagesOrDeserializationErrors = self
      .customInput(subscription.id, io)
      .map { msg =>
        val payload = msg.getPayload()
        val attributes = readAttributes(msg.getAttributeMap)

        readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Right(PubSubMessage(deserialized, attributes))
          case Failure(ex) =>
            Left(PubSubDeadLetter[T](payload, attributes, ex.getMessage))
        }
      }
    messagesOrDeserializationErrors.unzip
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

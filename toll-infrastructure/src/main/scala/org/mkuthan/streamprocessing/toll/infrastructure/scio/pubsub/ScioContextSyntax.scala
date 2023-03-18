package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.Failure
import scala.util.Success

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.shared.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes

private[pubsub] final class ScioContextOps(private val self: ScioContext) extends AnyVal {
  import ScioContextOps._

  def subscribeJsonFromPubsub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T],
      readConfiguration: JsonReadConfiguration = JsonReadConfiguration()
  ): (SCollection[PubsubMessage[T]], SCollection[PubsubDeadLetter[T]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .pipe(read => readConfiguration.configure(read))
      .fromSubscription(subscription.id)

    val messagesOrDeserializationErrors = self
      .customInput(subscription.id, io)
      .map { msg =>
        val payload = msg.getPayload
        val attributes = readAttributes(msg.getAttributeMap)

        readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Right(PubsubMessage(deserialized, attributes))
          case Failure(ex) =>
            Left(PubsubDeadLetter[T](payload, attributes, ex.getMessage))
        }
      }
    messagesOrDeserializationErrors.unzip
  }
}

private[pubsub] object ScioContextOps {
  private def readAttributes(attributes: JMap[String, String]): Map[String, String] =
    if (attributes == null) {
      Map.empty[String, String]
    } else {
      attributes.asScala.toMap
    }
}

trait ScioContextSyntax {
  import scala.language.implicitConversions

  implicit def pubsubScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}

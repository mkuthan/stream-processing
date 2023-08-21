package org.mkuthan.streamprocessing.infrastructure.pubsub

import scala.reflect.ClassTag
import scala.util.chaining._
import scala.util.Failure
import scala.util.Success

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.pubsub
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio._

private[pubsub] class ScioContextOps(private val self: ScioContext) {

  import ScioContextOps._

  def subscribeJsonFromPubsub[T <: AnyRef: Coder: ClassTag](
      id: IoIdentifier[T],
      subscription: PubsubSubscription[T],
      configuration: JsonReadConfiguration = JsonReadConfiguration()
  ): (SCollection[Message[T]], SCollection[PubsubDeadLetter[T]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .pipe(read => configuration.configure(read))
      .fromSubscription(subscription.id)

    val messagesOrDeserializationErrors = self
      .customInput(id.id, io)
      .withName(s"$id/Decode").map { msg =>
        val payload = msg.getPayload
        val attributes = readAttributes(msg.getAttributeMap)

        JsonSerde.readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Right(Message(deserialized, attributes))
          case Failure(ex) =>
            Left(pubsub.PubsubDeadLetter[T](id, payload, attributes, ex.getMessage))
        }
      }

    messagesOrDeserializationErrors
      .withName(s"$id/Handle errors")
      .unzip
  }
}

private[pubsub] object ScioContextOps extends Utils with PubsubCoders

trait ScioContextSyntax {
  import scala.language.implicitConversions

  implicit def pubsubScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}

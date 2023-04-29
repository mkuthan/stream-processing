package org.mkuthan.streamprocessing.shared.scio.pubsub

import scala.reflect.ClassTag
import scala.util.chaining._
import scala.util.Failure
import scala.util.Success

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.common.PubSubSubscription

private[pubsub] class ScioContextOps(private val self: ScioContext) {

  import ScioContextOps._

  def subscribeJsonFromPubsub[T <: AnyRef: Coder: ClassTag](
      ioIdentifier: IoIdentifier,
      subscription: PubSubSubscription[T],
      readConfiguration: JsonReadConfiguration = JsonReadConfiguration()
  ): (SCollection[PubsubMessage[T]], SCollection[PubsubDeadLetter[T]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .pipe(read => readConfiguration.configure(read))
      .fromSubscription(subscription.id)

    val messagesOrDeserializationErrors = self
      .customInput(ioIdentifier.id, io)
      .withName(s"$ioIdentifier/Decode").map { msg =>
        val payload = msg.getPayload
        val attributes = readAttributes(msg.getAttributeMap)

        JsonSerde.readJsonFromBytes[T](msg.getPayload) match {
          case Success(deserialized) =>
            Right(PubsubMessage(deserialized, attributes))
          case Failure(ex) =>
            Left(PubsubDeadLetter[T](payload, attributes, ex.getMessage))
        }
      }

    messagesOrDeserializationErrors
      .withName(s"$ioIdentifier/Handle errors")
      .unzip
  }
}

private[pubsub] object ScioContextOps extends Utils with PubsubCoders

trait ScioContextSyntax {
  import scala.language.implicitConversions

  implicit def pubsubScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
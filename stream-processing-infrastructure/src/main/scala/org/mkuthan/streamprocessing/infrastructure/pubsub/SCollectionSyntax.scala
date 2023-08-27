package org.mkuthan.streamprocessing.infrastructure.pubsub

import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.shared.json.JsonSerde

private[pubsub] class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[Message[T]]) {

  import SCollectionOps._

  def publishJsonToPubSub(
      id: IoIdentifier[T],
      topic: PubsubTopic[T],
      configuration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    val io = PubsubIO
      .writeMessages()
      .pipe(write => configuration.configure(write))
      .to(topic.id)

    val serializedMessages = self
      .withName(s"$id/Serialize")
      .map { msg =>
        val payload = JsonSerde.writeJsonAsBytes[T](msg.payload)
        val attributes = writeAttributes(msg.attributes)
        new BeamPubsubMessage(payload, attributes)
      }

    val _ = serializedMessages.saveAsCustomOutput(id.id, io)
  }
}

private[pubsub] object SCollectionOps extends Utils with PubsubCoders

private[pubsub] class SCollectionDeadLetterOps[T <: AnyRef: Coder](private val self: SCollection[PubsubDeadLetter[T]]) {
  def toDiagnostic(): SCollection[IoDiagnostic.Raw] =
    self.withTimestamp.map { case (deadLetter, ts) =>
      IoDiagnostic(ts, deadLetter.id, deadLetter.error)
    }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def pubsubSCollectionOps[T <: AnyRef: Coder](sc: SCollection[Message[T]]): SCollectionOps[T] =
    new SCollectionOps(sc)

  implicit def pubsubSCollectionDeadLetterOps[T <: AnyRef: Coder](sc: SCollection[PubsubDeadLetter[T]])
      : SCollectionDeadLetterOps[T] =
    new SCollectionDeadLetterOps(sc)
}

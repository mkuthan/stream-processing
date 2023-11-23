package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import scala.util.chaining._

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonWriteConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubTopic
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.shared.common.Message

private[syntax] trait PubsubSCollectionSyntax {

  implicit class PubsubSCollectionOps[T <: AnyRef: Coder](
      private val self: SCollection[Message[T]]
  ) {

    import com.spotify.scio.values.TestableSCollection._

    def publishJsonToPubsub(
        id: IoIdentifier[T],
        topic: PubsubTopic[T],
        configuration: JsonWriteConfiguration = JsonWriteConfiguration()
    ): Unit = {
      val io = PubsubIO
        .writeMessages()
        .pipe(write => configuration.configure(write))
        .to(topic.id)

      val _ = self.testableSaveAsCustomOutput(id.id) { in =>
        in
          .withName("Serialize")
          .map { msg =>
            val payload = JsonSerde.writeJsonAsBytes[T](msg.payload)
            val attributes = Utils.writeAttributes(msg.attributes)
            new BeamPubsubMessage(payload, attributes)
          }
          .internal.apply("Publish", io)
      }
    }
  }

  implicit class SCollectionDeadLetterOps[T <: AnyRef: Coder](
      private val self: SCollection[PubsubDeadLetter[T]]
  ) {
    def toDiagnostic(id: IoIdentifier[T]): SCollection[Diagnostic] =
      self
        .withName(id.id)
        .map(deadLetter => Diagnostic(id.id, deadLetter.error))
  }
}

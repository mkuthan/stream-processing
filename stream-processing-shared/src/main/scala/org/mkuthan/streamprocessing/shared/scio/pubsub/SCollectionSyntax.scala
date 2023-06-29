package org.mkuthan.streamprocessing.shared.scio.pubsub

import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.scio.common.PubsubTopic

private[pubsub] class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[PubsubMessage[T]]) {

  import SCollectionOps._

  def publishJsonToPubSub(
      id: IoIdentifier,
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

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def pubsubSCollectionOps[T <: AnyRef: Coder](sc: SCollection[PubsubMessage[T]]): SCollectionOps[T] =
    new SCollectionOps(sc)
}

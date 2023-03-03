package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic

final class PubSubSCollectionOps[T <: AnyRef](private val self: SCollection[PubSubMessage[T]]) extends AnyVal {

  implicit def messageCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  // TODO: add json in the method name
  def publishToPubSub(
      topic: PubSubTopic[T]
  )(implicit c: Coder[PubSubMessage[T]]): Unit = {
    import scala.jdk.CollectionConverters._

    val io = PubsubIO
      .writeMessages()
      .to(topic.id)

    // TODO: handle id/ts attributes, perhaps two functions T => id/ts attributes needed
    val _ = self
      .map { msg =>
        val payload = writeJsonAsBytes[T](msg.payload)
        val attributes = msg.attributes.asJava
        new PubsubMessage(payload, attributes)
      }.saveAsCustomOutput(topic.id, io)
  }

  // TODO: remove, smell
  def extractPayload()(implicit c: Coder[T]): SCollection[T] =
    self.map(_.payload)
}

trait SCollectionPubSubSyntax {
  implicit def pubSubSCollectionOps[T <: AnyRef](sc: SCollection[PubSubMessage[T]]): PubSubSCollectionOps[T] =
    new PubSubSCollectionOps(sc)
}

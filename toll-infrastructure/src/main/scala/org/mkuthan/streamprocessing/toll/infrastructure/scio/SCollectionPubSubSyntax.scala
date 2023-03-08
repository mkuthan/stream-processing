package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic

final class PubSubSCollectionOps[T <: AnyRef: Coder](private val self: SCollection[PubSubMessage[T]]) {

  implicit def pubsubMessageCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  def publishJsonToPubSub(
      topic: PubSubTopic[T]
  ): Unit = {
    import scala.jdk.CollectionConverters._

    val io = PubsubIO
      .writeMessages()
      .to(topic.id)

    val serializedMessages = self
      .map { msg =>
        val payload = writeJsonAsBytes[T](msg.payload)
        val attributes = msg.attributes.asJava
        new PubsubMessage(payload, attributes)
      }

    val _ = serializedMessages.saveAsCustomOutput(topic.id, io)
  }

  def extractPayload(): SCollection[T] =
    self.map(_.payload)
}

trait SCollectionPubSubSyntax {
  import scala.language.implicitConversions

  implicit def pubSubSCollectionOps[T <: AnyRef: Coder](sc: SCollection[PubSubMessage[T]]): PubSubSCollectionOps[T] =
    new PubSubSCollectionOps(sc)
}

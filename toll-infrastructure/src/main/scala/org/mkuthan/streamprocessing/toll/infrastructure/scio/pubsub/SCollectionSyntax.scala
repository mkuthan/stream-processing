package org.mkuthan.streamprocessing.toll.infrastructure.scio.pubsub

import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes

private[pubsub] final class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[PubsubMessage[T]]) {

  implicit def pubsubMessageCoder: Coder[BeamPubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  def publishJsonToPubSub(
      topic: PubSubTopic[T],
      writeConfiguration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    import scala.jdk.CollectionConverters._

    val io = PubsubIO
      .writeMessages()
      .pipe(write => writeConfiguration.configure(write))
      .to(topic.id)

    val serializedMessages = self
      .map { msg =>
        val payload = writeJsonAsBytes[T](msg.payload)
        val attributes = msg.attributes.asJava
        new BeamPubsubMessage(payload, attributes)
      }

    val _ = serializedMessages.saveAsCustomOutput(topic.id, io)
  }

  def extractPayload: SCollection[T] =
    self.map(_.payload)
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def pubsubSCollectionOps[T <: AnyRef: Coder](sc: SCollection[PubsubMessage[T]]): SCollectionOps[T] =
    new SCollectionOps(sc)
}

package org.mkuthan.streamprocessing.shared.scio.pubsub

import scala.util.chaining._

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.shared.configuration.PubSubTopic
import org.mkuthan.streamprocessing.shared.json.JsonSerde
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[pubsub] final class SCollectionOps[T <: AnyRef: Coder](private val self: SCollection[PubsubMessage[T]]) {

  import SCollectionOps._

  def publishJsonToPubSub(
      ioIdentifier: IoIdentifier,
      topic: PubSubTopic[T],
      writeConfiguration: JsonWriteConfiguration = JsonWriteConfiguration()
  ): Unit = {
    val io = PubsubIO
      .writeMessages()
      .pipe(write => writeConfiguration.configure(write))
      .to(topic.id)

    val serializedMessages = self
      .withName(s"$ioIdentifier/Serialize")
      .map { msg =>
        val payload = JsonSerde.writeJsonAsBytes[T](msg.payload)
        val attributes = writeAttributes(msg.attributes)
        new BeamPubsubMessage(payload, attributes)
      }

    val _ = serializedMessages.saveAsCustomOutput(ioIdentifier.id, io)
  }

  def extractPayload: SCollection[T] =
    self.map(_.payload)
}

private[pubsub] final object SCollectionOps extends Utils with PubsubCoders

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def pubsubSCollectionOps[T <: AnyRef: Coder](sc: SCollection[PubsubMessage[T]]): SCollectionOps[T] =
    new SCollectionOps(sc)
}

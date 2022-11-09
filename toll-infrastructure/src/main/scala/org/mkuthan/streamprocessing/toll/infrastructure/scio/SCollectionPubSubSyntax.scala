package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJson
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubTopic

final class PubSubSCollectionOps[T <: AnyRef](private val self: SCollection[T]) extends AnyVal {
  def publishToPubSub(
      topic: PubSubTopic[T]
  )(implicit c: Coder[T]): Unit = {
    val io = PubsubIO.writeStrings().to(topic.id)
    self
      .map(writeJson[T])
      .saveAsCustomOutput(topic.id, io)
    ()
  }
}

trait SCollectionPubSubSyntax {
  implicit def pubSubSCollectionOps[T <: AnyRef](sc: SCollection[T]): PubSubSCollectionOps[T] =
    new PubSubSCollectionOps(sc)
}

package org.mkuthan.streamprocessing.toll.infrastructure

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.values.SCollection
import org.mkuthan.streamprocessing.toll.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.configuration.PubSubTopic

object PubSubRepository {
  def subscribe[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T]
  )(implicit sc: ScioContext): SCollection[T] =
    sc
      .read(PubsubIO.string(subscription.id))(PubsubIO.ReadParam(PubsubIO.Subscription))
      .map(JsonSerde.read[T])

  def publish[T <: AnyRef: Coder](
      topic: PubSubTopic[T],
      data: SCollection[T]
  ): Unit =
    data
      .map(JsonSerde.write[T])
      .write(PubsubIO.string(topic.id))(PubsubIO.WriteParam())
}

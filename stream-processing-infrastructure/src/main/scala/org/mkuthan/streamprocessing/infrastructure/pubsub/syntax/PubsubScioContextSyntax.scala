package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import scala.reflect.ClassTag
import scala.util.chaining._
import scala.util.Failure
import scala.util.Success

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.json.JsonSerde
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubSubscription
import org.mkuthan.streamprocessing.shared.common.Message

private[syntax] trait PubsubScioContextSyntax {

  implicit class PubsubScioContextOps(private val self: ScioContext) {

    import com.spotify.scio.TestableScioContext._

    def subscribeJsonFromPubsub[T <: AnyRef: Coder: ClassTag](
        id: IoIdentifier[T],
        subscription: PubsubSubscription[T],
        configuration: JsonReadConfiguration = JsonReadConfiguration()
    ): SCollection[PubsubResult[T]] = {
      val io = PubsubIO
        .readMessagesWithAttributes()
        .pipe(read => configuration.configure(read))
        .fromSubscription(subscription.id)

      self.testableCustomInput(id.id) { in =>
        self.wrap(in.apply("Subscribe", io))
          .withName("Deserialize")
          .map { msg =>
            val payload = msg.getPayload
            val attributes = Utils.readAttributes(msg.getAttributeMap)

            JsonSerde.readJsonFromBytes[T](msg.getPayload) match {
              case Success(deserialized) =>
                Right(Message(deserialized, attributes))
              case Failure(ex) =>
                Left(PubsubDeadLetter[T](payload, attributes, ex.getMessage))
            }
          }
      }
    }
  }
}

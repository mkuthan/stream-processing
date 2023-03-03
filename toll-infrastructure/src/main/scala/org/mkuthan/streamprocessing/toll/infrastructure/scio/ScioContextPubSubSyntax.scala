package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.readJsonFromBytes
import org.mkuthan.streamprocessing.toll.shared.configuration.PubSubSubscription

final class PubSubScioContextOps(private val self: ScioContext) extends AnyVal {
  import PubSubScioContextOps._

  def subscribeJsonFromPubSub[T <: AnyRef: Coder: Manifest](
      subscription: PubSubSubscription[T],
      idAttribute: Option[String] = None,
      tsAttribute: Option[String] = None
  ): (SCollection[PubSubMessage[T]], SCollection[Array[Byte]]) = {
    val io = PubsubIO
      .readMessagesWithAttributes()
      .fromSubscription(subscription.id)

    idAttribute.foreach(io.withIdAttribute(_))
    tsAttribute.foreach(io.withTimestampAttribute(_))

    val dlq = SideOutput[Array[Byte]]()

    val (results, sideOutputs) = self
      .customInput(subscription.id, io)
      .withSideOutputs(dlq)
      .flatMap { (msg, ctx) =>
        val pubSubMessage = for {
          payload <- readJsonFromBytes[T](msg.getPayload)
          attributes <- readAttributes(msg.getAttributeMap)
        } yield PubSubMessage(payload, attributes)

        pubSubMessage match {
          case Success(m) => Some(m)
          case Failure(_) => // TODO: put error message into DLQ
            ctx.output(dlq, msg.getPayload) // TODO: what about attributes
            None
        }
      }

    (results, sideOutputs(dlq))
  }
}

object PubSubScioContextOps {
  private def readAttributes(attributes: java.util.Map[String, String]): Try[Map[String, String]] = {
    import scala.jdk.CollectionConverters._

    val results = if (attributes == null) {
      Map.empty[String, String]
    } else {
      attributes.asScala.toMap
    }
    Success(results)
  }
}

trait ScioContextPubSubSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): PubSubScioContextOps = new PubSubScioContextOps(sc)
}

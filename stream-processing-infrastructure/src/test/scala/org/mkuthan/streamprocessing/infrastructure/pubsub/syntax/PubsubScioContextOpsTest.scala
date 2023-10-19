package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import org.joda.time.Instant
import org.scalactic.Equality
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow
import org.scalatest.EitherValues._

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.pubsub.JsonReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.pubsub.NamedIdAttribute
import org.mkuthan.streamprocessing.infrastructure.pubsub.NamedTimestampAttribute
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubSubscription
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures
import org.mkuthan.streamprocessing.infrastructure.IntegrationTestFixtures.SampleClass
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.common.RandomString._
import org.mkuthan.streamprocessing.test.gcp.GcpTestPatience
import org.mkuthan.streamprocessing.test.gcp.PubsubClient._
import org.mkuthan.streamprocessing.test.gcp.PubsubContext
import org.mkuthan.streamprocessing.test.scio.InMemorySink
import org.mkuthan.streamprocessing.test.scio.IntegrationTestScioContext

@Slow
class PubsubScioContextOpsTest extends AnyFlatSpec with Matchers
    with Eventually with GcpTestPatience
    with IntegrationTestScioContext
    with IntegrationTestFixtures
    with PubsubContext {

  implicit def pubsubDeadLetterEquality[T]: Equality[PubsubDeadLetter[T]] = (self: PubsubDeadLetter[T], other: Any) =>
    other match {
      case o: PubsubDeadLetter[_] =>
        self.payload.sameElements(o.payload) &&
        self.attributes == o.attributes &&
        self.error.startsWith(o.error)
      case _ => false
    }

  behavior of "Pubsub ScioContext syntax"

  it should "subscribe JSON" in withScioContext { implicit sc =>
    options.setBlockOnRun(false)

    withTopic { topic =>
      withSubscription(topic) { subscription =>
        publishMessages(
          topic,
          (SampleJson1, SampleMap1),
          (SampleJson2, SampleMap2)
        )

        val results = sc.subscribeJsonFromPubsub(
          IoIdentifier[SampleClass]("any-id"),
          PubsubSubscription[SampleClass](subscription)
        )
        val sink = InMemorySink(results)
        val run = sc.run()

        eventually {
          sink.toSeq should contain.only(
            Right(Message(SampleObject1, SampleMap1)),
            Right(Message(SampleObject2, SampleMap2))
          )
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "subscribe invalid JSON and put into DLQ" in withScioContext { implicit sc =>
    options.setBlockOnRun(false)

    withTopic { topic =>
      withSubscription(topic) { subscription =>
        publishMessages(topic, (InvalidJson, SampleMap1))

        val results = sc.subscribeJsonFromPubsub(
          IoIdentifier[SampleClass]("any-id"),
          PubsubSubscription[SampleClass](subscription)
        )
        val sink = InMemorySink(results)
        val run = sc.run()

        val expectedDeadLetter = PubsubDeadLetter[SampleClass](
          payload = InvalidJson,
          attributes = SampleMap1,
          error = "Unrecognized token 'invalid'"
        )

        eventually {
          sink.toElement.left.value should equal(expectedDeadLetter)
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "subscribe JSON with id attribute" in withScioContext { implicit sc =>
    options.setBlockOnRun(false)

    withTopic { topic =>
      withSubscription(topic) { subscription =>
        val attributes = SampleMap1 + (NamedIdAttribute.Default.name -> randomString())
        val messagePrototype = (SampleJson1, attributes)

        publishMessages(topic, Seq.fill(10)(messagePrototype): _*)

        val results = sc.subscribeJsonFromPubsub(
          IoIdentifier[SampleClass]("any-id"),
          subscription = PubsubSubscription[SampleClass](subscription),
          configuration = JsonReadConfiguration().withIdAttribute(NamedIdAttribute.Default)
        )
        val messagesSink = InMemorySink(results)
        val run = sc.run()

        eventually {
          messagesSink.toElement should be(Right(Message(SampleObject1, attributes)))
        }

        run.pipelineResult.cancel()
      }
    }
  }

  it should "subscribe JSON with timestamp attribute" in withScioContext { implicit sc =>
    options.setBlockOnRun(false)

    withTopic { topic =>
      withSubscription(topic) { subscription =>
        val timestamp = Instant.now()
        val attributes = SampleMap1 + (NamedTimestampAttribute.Default.name -> timestamp.toString)

        publishMessages(topic, (SampleJson1, attributes))

        val results = sc.subscribeJsonFromPubsub(
          IoIdentifier[SampleClass]("any-id"),
          subscription = PubsubSubscription[SampleClass](subscription),
          configuration = JsonReadConfiguration().withTimestampAttribute(NamedTimestampAttribute.Default)
        )

        val messagesSink = InMemorySink(results.withTimestamp)

        val run = sc.run()

        eventually {
          val (msg, ts) = messagesSink.toElement
          msg should be(Right(Message(SampleObject1, attributes)))
          ts should be(timestamp)
        }

        run.pipelineResult.cancel()
      }
    }
  }
}

package org.mkuthan.streamprocessing.toll.infrastructure.scio

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.gcp.PubSubClient
import org.mkuthan.streamprocessing.shared.test.scio.GcpScioContext
import org.mkuthan.streamprocessing.shared.test.scio.PubSubScioContext
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

class ScioContextPubSubSyntaxTest extends AnyFlatSpec
    with Matchers
    with PubSubScioContext
    with PubSubClient
    with ScioContextPubSubSyntax {

  import IntegrationTestFixtures._

  behavior of "ScioContextPubSubSyntaxTest"

  it should "subscribe to topic" in withScioContext { sc =>
    withTopic[ComplexClass] { topic =>
      withSubscription[ComplexClass](topic.id) { subscription =>
        publishMessage(topic.id, JsonSerde.write(complexObject1), JsonSerde.write(complexObject2))

        val results = sc
          .subscribeToPubSub(subscription)
          .debug()

      // sc.run().waitUntilFinish()

      // results should containInAnyOrder(Seq(complexObject1, complexObject2))
      }
    }

  // https://github.com/apache/beam/blob/09bbb48187301f18bec6d9110741c69b955e2b5a/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/pubsub/PubsubReadIT.java
  // https://github.com/mozilla/gcp-ingestion/blob/fa98ac0c8fa09b5671a961062e6cf0985ec48b0e/ingestion-beam/src/test/java/com/mozilla/telemetry/integration/PubsubIntegrationTest.java
  // https://github.com/damccorm/beam-pr-bot-demo/blob/a3974531e41fa7a8303f2507625d61352ebd1b9d/examples/java/src/test/java/org/apache/beam/examples/complete/kafkatopubsub/KafkaToPubsubE2ETest.java
  // https://www.mail-archive.com/search?l=user%40beam.apache.org&q=subject:%22Terminating+a+streaming+integration+test%22&o=newest&f=1
  // https://github.com/apache/beam/blob/de1c14777d3c6a1231361db12f3a0b9fd3b84b3e/runners/google-cloud-dataflow-java/src/main/java/org/apache/beam/runners/dataflow/TestDataflowRunner.java#L145
  }
}

package org.mkuthan.streamprocessing.toll.infrastructure.scio

import java.util.UUID

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.ScioContext

import org.apache.beam.runners.direct.DirectOptions
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll

import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

class ScioContextPubSubSyntaxTest extends PipelineSpec
    with BeforeAndAfterAll
    with PubSubClient
    with ScioContextPubSubSyntax {

  private val options = PipelineOptionsFactory.create()
  options.as(classOf[DirectOptions]).setBlockOnRun(false)

  val topicName = generateTopicName()
  val subscriptionName = generateSubscriptionName()

  val pubSubSubscription = PubSubSubscription[AnyCaseClass](subscriptionName)

  private val record1 = AnyCaseClass("foo", 1)
  private val record2 = AnyCaseClass("foo", 2)

  override def beforeAll(): Unit = {
    createTopic(topicName)
    createSubscription(topicName, subscriptionName)
  }

  override def afterAll(): Unit = {
    deleteSubscription(subscriptionName)
    deleteTopic(topicName)
  }

  behavior of "ScioContextPubSubSyntaxTest"

  ignore should "subscribe to topic" in {
    val sc = ScioContext(options)
    publishMessage(topicName, JsonSerde.write(record1), JsonSerde.write(record2))

    val results = sc
      .subscribeToPubSub(pubSubSubscription)

    println("DUPA: " + results.debug())

    results should containInAnyOrder(Seq(record1, record2))
    val result = sc.run()

    // https://github.com/apache/beam/blob/09bbb48187301f18bec6d9110741c69b955e2b5a/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/pubsub/PubsubReadIT.java
    // https://github.com/mozilla/gcp-ingestion/blob/fa98ac0c8fa09b5671a961062e6cf0985ec48b0e/ingestion-beam/src/test/java/com/mozilla/telemetry/integration/PubsubIntegrationTest.java
    // https://github.com/damccorm/beam-pr-bot-demo/blob/a3974531e41fa7a8303f2507625d61352ebd1b9d/examples/java/src/test/java/org/apache/beam/examples/complete/kafkatopubsub/KafkaToPubsubE2ETest.java
    // https://www.mail-archive.com/search?l=user%40beam.apache.org&q=subject:%22Terminating+a+streaming+integration+test%22&o=newest&f=1
    // https://github.com/apache/beam/blob/de1c14777d3c6a1231361db12f3a0b9fd3b84b3e/runners/google-cloud-dataflow-java/src/main/java/org/apache/beam/runners/dataflow/TestDataflowRunner.java#L145
  }
}

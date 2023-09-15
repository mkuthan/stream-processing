package org.mkuthan.streamprocessing.toll.domain.booth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio._

class TollBoothEntryTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothEntryFixture {

  import TollBoothEntry._

  behavior of "TollBoothEntry"

  it should "decode valid payload into TollBoothEntry" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothEntry.Payload]]
      .addElementsAtWatermarkTime(Message(anyTollBoothEntryPayload))
      .advanceWatermarkToInfinity()

    val (results, dlq) = decodePayload(sc.testUnbounded(inputs))

    results should containSingleValue(anyTollBoothEntry)
    dlq should beEmpty
  }

  it should "put invalid payload into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothEntry.Payload]]
        .addElementsAtWatermarkTime(Message(tollBoothEntryPayloadInvalid))
        .advanceWatermarkToInfinity()

      val (results, dlq) = decodePayload(sc.testUnbounded(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothEntryDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothEntry.DlqCounter).attempted shouldBe 1
  }

}

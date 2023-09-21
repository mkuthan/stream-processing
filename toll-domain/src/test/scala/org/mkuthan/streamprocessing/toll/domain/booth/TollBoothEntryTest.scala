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

  it should "decode valid message into TollBoothEntry" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothEntry.Payload]]
      .addElementsAtTime(anyTollBoothEntryPayload.entry_time, Message(anyTollBoothEntryPayload))
      .advanceWatermarkToInfinity()

    val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

    results.withTimestamp should containSingleValueAtTime(anyTollBoothEntry.entryTime, anyTollBoothEntry)
    dlq should beEmpty
  }

  it should "put invalid message into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothEntry.Payload]]
        .addElementsAtTime(tollBoothEntryPayloadInvalid.entry_time, Message(tollBoothEntryPayloadInvalid))
        .advanceWatermarkToInfinity()

      val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

      results should beEmpty
      dlq.withTimestamp should containSingleValueAtTime(
        tollBoothEntryPayloadInvalid.entry_time,
        tollBoothEntryDecodingError
      )
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothEntry.DlqCounter).attempted shouldBe 1
  }

  it should "decode valid record into TollBoothEntry" in runWithScioContext { sc =>
    val inputs = boundedTestCollectionOf[TollBoothEntry.Record]
      .addElementsAtMinimumTime(anyTollBoothEntryRecord)
      .advanceWatermarkToInfinity()

    val results = decodeRecord(sc.testBounded(inputs))

    results.withTimestamp should containSingleValueAtTime(anyTollBoothEntry.entryTime, anyTollBoothEntry)
  }

}

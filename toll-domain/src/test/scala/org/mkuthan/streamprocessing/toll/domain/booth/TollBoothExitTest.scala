package org.mkuthan.streamprocessing.toll.domain.booth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio._

class TollBoothExitTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothExitFixture {

  import TollBoothExit._

  behavior of "TollBoothExit"

  it should "decode valid message into TollBoothExit" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
      .addElementsAtTime(anyTollBoothExitPayload.exit_time, Message(anyTollBoothExitPayload))
      .advanceWatermarkToInfinity()

    val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

    results.withTimestamp should containSingleValueAtTime(anyTollBoothExit.exitTime, anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid message into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
        .addElementsAtTime(tollBoothExitPayloadInvalid.exit_time, Message(tollBoothExitPayloadInvalid))
        .advanceWatermarkToInfinity()

      val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

      results should beEmpty
      dlq.withTimestamp should containSingleValueAtTime(
        tollBoothExitPayloadInvalid.exit_time,
        tollBoothExitDecodingError
      )
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

  it should "decode valid record into TollBoothExit" in runWithScioContext { sc =>
    val inputs = boundedTestCollectionOf[TollBoothExit.Record]
      .addElementsAtMinimumTime(anyTollBoothExitRecord)
      .advanceWatermarkToInfinity()

    val results = decodeRecord(sc.testBounded(inputs))

    results.withTimestamp should containSingleValueAtTime(anyTollBoothExit.exitTime, anyTollBoothExit)
  }

}

package org.mkuthan.streamprocessing.toll.domain.booth

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class TollBoothExitTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothExitFixture {

  import TollBoothExit._

  behavior of "TollBoothExit"

  it should "decode valid message into TollBoothExit" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
      .addElementsAtTime(
        anyTollBoothExitMessage.attributes(TollBoothExit.TimestampAttribute),
        anyTollBoothExitMessage
      ).advanceWatermarkToInfinity()

    val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

    results.withTimestamp should containElementsAtTime(anyTollBoothExit.exitTime, anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid message into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
        .addElementsAtTime(
          invalidTollBoothExitMessage.attributes(TollBoothExit.TimestampAttribute),
          invalidTollBoothExitMessage
        ).advanceWatermarkToInfinity()

      val (results, dlq) = decodeMessage(sc.testUnbounded(inputs))

      results should beEmpty
      dlq.withTimestamp should containElementsAtTime(
        invalidTollBoothExitMessage.attributes(TollBoothExit.TimestampAttribute),
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

    results.withTimestamp should containElementsAtTime(anyTollBoothExit.exitTime, anyTollBoothExit)
  }

}

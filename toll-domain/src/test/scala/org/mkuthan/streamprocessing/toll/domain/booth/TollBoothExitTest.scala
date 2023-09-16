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

  it should "decode valid payload into TollBoothExit" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
      .addElementsAtWatermarkTime(Message(anyTollBoothExitPayload))
      .advanceWatermarkToInfinity()

    val (results, dlq) = decodePayload(sc.testUnbounded(inputs))

    results should containSingleValue(anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid payload into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Payload]]
        .addElementsAtWatermarkTime(Message(tollBoothExitPayloadInvalid))
        .advanceWatermarkToInfinity()

      val (results, dlq) = decodePayload(sc.testUnbounded(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothExitDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

  it should "decode valid record into TollBoothExit" in runWithScioContext { sc =>
    val inputs = boundedTestCollectionOf[TollBoothExit.Record]
      .addElementsAtMinimumTime(anyTollBoothExitRecord)
      .build()

    val results = decodeRecord(sc.testBounded(inputs))

    results should containSingleValue(anyTollBoothExit)
  }

}

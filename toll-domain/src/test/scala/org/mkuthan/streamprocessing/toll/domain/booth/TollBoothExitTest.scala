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

  it should "decode valid TollBoothExit into raw" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Raw]]
      .addElementsAtMinimumTime(Message(anyTollBoothExitRaw))
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testUnbounded(inputs))

    results should containSingleValue(anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid TollBoothExit into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = unboundedTestCollectionOf[Message[TollBoothExit.Raw]]
        .addElementsAtMinimumTime(Message(tollBoothExitRawInvalid))
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testUnbounded(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothExitDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

}

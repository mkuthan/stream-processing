package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.TestStreamScioContext
import org.mkuthan.streamprocessing.test.scio.TestScioContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class TollBoothExitTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothExitFixture {

  import TollBoothExit._

  behavior of "TollBoothExit"

  it should "decode valid TollBoothExit into raw" in runWithScioContext { sc =>
    val inputs = testStreamOf[TollBoothExit.Raw]
      .addElements(anyTollBoothExitRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid TollBoothExit into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = testStreamOf[TollBoothExit.Raw]
        .addElements(tollBoothExitRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothExitDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

}

package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

final class TollBoothExitTest extends PipelineSpec with TollBoothExitFixture {

  import TollBoothExit._

  behavior of "TollBoothExit"

  it should "decode valid TollBoothExit into raw" in runWithContext { sc =>
    val inputs = testStreamOf[TollBoothExit.Raw]
      .addElements(anyTollBoothExitRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothExit)
    dlq should beEmpty
  }

  it should "put invalid TollBoothExit into DLQ" in {
    val run = runWithContext { sc =>
      val inputs = testStreamOf[TollBoothExit.Raw]
        .addElements(tollBoothExitRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothExitRawInvalid)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

}

package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

final class TollBoothExitTest extends PipelineSpec with TollBoothExitFixture {

  import TollBoothExit._

  "Valid TollBoothExit" should "be decoded" in runWithContext { sc =>
    val inputs = testStreamOf[TollBoothExit.Raw]
      .addElements(anyTollBoothExitRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothExit)
    dlq should beEmpty
  }

  "Invalid TollBoothExit" should "go to DLQ" in {
    val run = runWithContext { sc =>
      val invalidTollBoothExitRaw = anyTollBoothExitRaw.copy(exit_time = "invalid time")

      val inputs = testStreamOf[TollBoothExit.Raw]
        .addElements(invalidTollBoothExitRaw)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(invalidTollBoothExitRaw)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothExit.DlqCounter).attempted shouldBe 1
  }

}

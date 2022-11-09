package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

final class TollBoothEntryTest extends PipelineSpec with TollBoothEntryFixture {

  import TollBoothEntry._

  behavior of "TollBoothEntry"

  it should "decode valid TollBoothEntry into raw" in runWithContext { sc =>
    val inputs = testStreamOf[TollBoothEntry.Raw]
      .addElements(anyTollBoothEntryRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothEntry)
    dlq should beEmpty
  }

  it should "put invalid TollBoothEntry into DLQ" in {
    val run = runWithContext { sc =>
      val inputs = testStreamOf[TollBoothEntry.Raw]
        .addElements(tollBoothEntryRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothEntryRawInvalid)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothEntry.DlqCounter).attempted shouldBe 1
  }

}

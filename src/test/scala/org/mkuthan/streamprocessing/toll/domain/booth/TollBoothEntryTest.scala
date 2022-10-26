package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext

final class TollBoothEntryTest extends PipelineSpec with TollBoothEntryFixture {

  import TollBoothEntry._

  "Valid TollBoothEntry" should "be decoded" in runWithContext { sc =>
    val inputs = testStreamOf[TollBoothEntry.Raw]
      .addElements(anyTollBoothEntryRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothEntry)
    dlq should beEmpty
  }

  "Invalid TollBoothEntry" should "go to DLQ" in {
    val run = runWithContext { sc =>
      val invalidTollBoothEntryRaw = anyTollBoothEntryRaw.copy(entry_time = "invalid time")

      val inputs = testStreamOf[TollBoothEntry.Raw]
        .addElements(invalidTollBoothEntryRaw)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(invalidTollBoothEntryRaw)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothEntry.DlqCounter).attempted shouldBe 1
  }

}

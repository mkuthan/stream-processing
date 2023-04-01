package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.TestStreamScioContext

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.scio.TestScioContext

final class TollBoothEntryTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothEntryFixture {

  import TollBoothEntry._

  behavior of "TollBoothEntry"

  it should "decode valid TollBoothEntry into raw" in runWithScioContext { sc =>
    val inputs = testStreamOf[TollBoothEntry.Raw]
      .addElements(anyTollBoothEntryRaw)
      .advanceWatermarkToInfinity()

    val (results, dlq) = decode(sc.testStream(inputs))

    results should containSingleValue(anyTollBoothEntry)
    dlq should beEmpty
  }

  it should "put invalid TollBoothEntry into DLQ" in {
    val run = runWithScioContext { sc =>
      val inputs = testStreamOf[TollBoothEntry.Raw]
        .addElements(tollBoothEntryRawInvalid)
        .advanceWatermarkToInfinity()

      val (results, dlq) = decode(sc.testStream(inputs))

      results should beEmpty
      dlq should containSingleValue(tollBoothEntryDecodingError)
    }

    val result = run.waitUntilDone()
    result.counter(TollBoothEntry.DlqCounter).attempted shouldBe 1
  }

}

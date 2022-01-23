package org.mkuthan.examples.streaming.beam

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestStreamScioContext
import com.spotify.scio.testing.testStreamOf

class WordCountTest extends PipelineSpec {

  "Words" should "be counted" in runWithContext { sc =>
    val words = testStreamOf[String]
      .addElements("foo", "bar", "foo")
      .advanceWatermarkToInfinity()

    val results = WordCount.wordCount(sc.testStream(words))

    results should containInAnyOrder(Seq(("foo", 2L), ("bar", 1L)))
  }
}

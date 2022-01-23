package org.mkuthan.examples.streaming.beam

import com.spotify.scio.values.SCollection
import org.joda.time.Duration

object WordCount {

  def wordCount(words: SCollection[String]): SCollection[(String, Long)] = {
    words
      .withFixedWindows(Duration.standardMinutes(10L))
      .countByValue
  }

}

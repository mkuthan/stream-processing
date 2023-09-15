package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.ContextAndArgs

object TollBatchApplication {
  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val _ = sc.run()
  }

}

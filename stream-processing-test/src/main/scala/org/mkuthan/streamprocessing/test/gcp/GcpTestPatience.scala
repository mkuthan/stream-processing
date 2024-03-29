package org.mkuthan.streamprocessing.test.gcp

import org.scalatest.concurrent.AbstractPatienceConfiguration
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span

trait GcpTestPatience extends AbstractPatienceConfiguration { this: PatienceConfiguration =>

  private val defaultPatienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(30, Seconds)),
      interval = scaled(Span(1500, Millis))
    )

  implicit abstract override val patienceConfig: PatienceConfig = defaultPatienceConfig
}

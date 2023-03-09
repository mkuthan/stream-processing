package org.mkuthan.streamprocessing.toll.infrastructure.scio.core

import com.spotify.scio.testing.PipelineSpec

class SCollectionSyntaxTest extends PipelineSpec with SCollectionSyntax {
  behavior of "Core SCollection syntax"

  it should "unzip Either" in runWithContext { sc =>
    val (right, left) = sc
      .parallelize[Either[String, String]](Seq(Right("r1"), Left("l1"), Right("r2"), Left("l2"), Right("r3")))
      .unzip

    right should containInAnyOrder(Seq("r1", "r2", "r3"))
    left should containInAnyOrder(Seq("l1", "l2"))
  }
}

package org.mkuthan.streamprocessing.lookupjoin

import scala.jdk.CollectionConverters._

import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.joda.time.Instant

class MaxInstantFn extends CombineFn[Instant, Instant, Instant] {

  override def createAccumulator(): Instant = Instant.EPOCH

  override def addInput(accumulator: Instant, input: Instant): Instant =
    if (accumulator.isAfter(input)) accumulator else input

  override def mergeAccumulators(accumulators: java.lang.Iterable[Instant]): Instant =
    accumulators.asScala.foldLeft(createAccumulator())(addInput)

  override def extractOutput(accumulator: Instant): Instant = accumulator
}

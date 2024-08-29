package org.mkuthan.streamprocessing.lookupjoin

import org.joda.time.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext

class ExampleLookupJoinTest extends AnyFlatSpec with Matchers with TestScioContext with TableDrivenPropertyChecks {

  import ExampleLookupJoin._

  private val anyValue = Value("any id", "any value")
  private val anyLookup = Lookup("any id", "any lookup")
  private val someLookup = Option(anyLookup)
  private val noneLookup = Option.empty[Lookup]

  private val oneMinute = Duration.standardMinutes(1)
  private val tenSeconds = Duration.standardSeconds(10)

  behavior of "LookupCacheDoFn"

  it should "join Value with Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyLookup)
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyValue)
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "12:00:00",
      (anyValue, someLookup)
    )
  }

  it should "not join Value with Lookup when different ids" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyLookup.copy(id = "non-matching id"))
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyValue)
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "12:00:00",
      (anyValue, noneLookup)
    )
  }

  it should "not join late Value with Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .addElementsAtTime("12:00:00", anyLookup)
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .advanceWatermarkTo("12:10:01")
      .addElementsAtTime("12:10:01", anyValue)
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "12:10:01",
      (anyValue, noneLookup)
    )
  }

  it should "not join too early Value with Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyLookup)
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .addElementsAtTime("11:49:59", anyValue)
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "11:49:59",
      (anyValue, noneLookup)
    )
  }

  it should "join early Value with Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyLookup)
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .addElementsAtTime("11:59:59", anyValue)
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "12:00:00",
      (anyValue, someLookup)
    )
  }

  it should "join two Values with a single Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyLookup)
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .addElementsAtTime("12:00:00", anyValue.copy(name = "first value"))
      .addElementsAtTime("12:00:00", anyValue.copy(name = "second value"))
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      "12:00:00",
      (anyValue.copy(name = "first value"), someLookup),
      (anyValue.copy(name = "second value"), someLookup)
    )
  }

  it should "emit frequent Values after their time to live expires" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .advanceWatermarkTo("12:00:00")
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .advanceWatermarkTo("12:00:00")
      .addElementsAtTime("12:00:00", anyValue.copy(name = "first value"))
      .addElementsAtTime("12:00:40", anyValue.copy(name = "second value"))
      .advanceWatermarkTo("12:01:01") // ensure that the first values release timer should fire
      .addElementsAtTime("12:01:20", anyValue.copy(name = "third value"))
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      ("12:00:40", (anyValue.copy(name = "first value"), noneLookup)),
      ("12:00:40", (anyValue.copy(name = "second value"), noneLookup)),
      ("12:01:20", (anyValue.copy(name = "third value"), noneLookup))
    )
  }

  it should "join additional Value with updated Lookup" in runWithScioContext { sc =>
    val lookups = unboundedTestCollectionOf[Lookup]
      .addElementsAtTime("12:00:00", anyLookup.copy(name = "initial lookup"))
      .advanceWatermarkTo("12:01:00") // ensure that first Value is emitted
      .addElementsAtTime("12:02:00", anyLookup.copy(name = "updated lookup"))
      .advanceWatermarkToInfinity()

    val values = unboundedTestCollectionOf[Value]
      .advanceWatermarkTo("12:00:10") // ensure that first Lookup has been observed
      .addElementsAtTime("12:00:30", anyValue.copy(name = "first value"))
      .advanceWatermarkTo("12:02:10") // ensure that second Lookup has been observed
      .addElementsAtTime("12:02:30", anyValue.copy(name = "second value"))
      .advanceWatermarkToInfinity()

    val results = lookupJoin(
      sc.testUnbounded(values),
      sc.testUnbounded(lookups),
      oneMinute,
      tenSeconds
    )

    results.withTimestamp should containElementsAtTime(
      ("12:00:30", (anyValue.copy(name = "first value"), someLookup.map(_.copy(name = "initial lookup")))),
      ("12:02:30", (anyValue.copy(name = "second value"), someLookup.map(_.copy(name = "updated lookup"))))
    )
  }
}

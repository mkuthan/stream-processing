package org.mkuthan.streamprocessing.toll.infrastructure.json

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import magnolify.scalacheck.auto._
import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.mkuthan.streamprocessing.shared.test.common.JodaTimeArbitrary

object JsonSerdeTest extends JodaTimeArbitrary {

  case class SampleClass(
      string: String,
      optionString: Option[String],
      int: Int,
      double: Double,
      bigInt: BigInt,
      bigDecimal: BigDecimal,
      dateTime: DateTime,
      instant: Instant,
      localDateTime: LocalDateTime,
      localDate: LocalDate,
      localTime: LocalTime
  )

  implicit val sampleClassArbitrary = implicitly[Arbitrary[SampleClass]]
}

final class JsonSerdeTest extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks {

  import JsonSerde._
  import JsonSerdeTest._

  override implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 1000)

  behavior of "JsonSerde"

  it should "serialize and deserialize" in {
    forAll { sample: SampleClass =>
      val serialized = writeJsonAsString(sample)
      val deserialized = readJsonFromString[SampleClass](serialized).success.value
      deserialized shouldMatchTo sample
    }
  }

  it should "not deserialize unknown object" in {
    val unknownObjectJson = """{"unknownField":"a"}"""
    val result = readJsonFromString[SampleClass](unknownObjectJson)
    result.failure.exception.getMessage should startWith("Unrecognized field \"unknownField\"")
  }
}

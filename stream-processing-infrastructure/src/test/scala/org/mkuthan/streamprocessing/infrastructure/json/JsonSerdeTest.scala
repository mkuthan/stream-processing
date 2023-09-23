package org.mkuthan.streamprocessing.infrastructure.json

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import magnolify.scalacheck.auto._
import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.mkuthan.streamprocessing.test.common.JodaTimeArbitrary

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

  case class SingleFieldClass(f1: String)

  case class TwoFieldsClass(f1: String, f2: String)

  case class ParametrizedClass[T](f: T)
}

class JsonSerdeTest extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks {

  import JsonSerde._
  import JsonSerdeTest._

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  behavior of "JsonSerde"

  it should "serialize and deserialize sample type using string" in {
    forAll { sample: SampleClass =>
      val serialized = writeJsonAsString(sample)
      val deserialized = readJsonFromString[SampleClass](serialized)
      deserialized.success.value shouldMatchTo sample
    }
  }

  it should "serialize and deserialize sample type using bytes" in {
    forAll { sample: SampleClass =>
      val serialized = writeJsonAsBytes(sample)
      val deserialized = readJsonFromBytes[SampleClass](serialized)
      deserialized.success.value shouldMatchTo sample
    }
  }

  it should "serialize and deserialize parametrized type using string" in {
    val parametrized = ParametrizedClass("field")
    val serialized = writeJsonAsString(parametrized)
    val deserialized = readJsonFromString[ParametrizedClass[String]](serialized)
    deserialized.success.value shouldMatchTo parametrized
  }

  it should "serialize and deserialize parametrized type using bytes" in {
    val parametrized = ParametrizedClass("field")
    val serialized = writeJsonAsBytes(parametrized)
    val deserialized = readJsonFromBytes[ParametrizedClass[String]](serialized)
    deserialized.success.value shouldMatchTo parametrized
  }

  it should "deserialize unknown field" in {
    val twoFields = TwoFieldsClass("f1", "f2")
    val twoFieldsSerialized = writeJsonAsString(twoFields)
    val result = readJsonFromString[SingleFieldClass](twoFieldsSerialized)
    result.success.value shouldMatchTo SingleFieldClass("f1")
  }

  it should "not deserialize null for required fields" in {
    val nullJson = "{}"
    val result = readJsonFromString[SampleClass](nullJson)
    result.failure.exception.getMessage should startWith("Null value for creator property 'string'")
  }
}

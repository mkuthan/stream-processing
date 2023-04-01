package org.mkuthan.streamprocessing.toll.infrastructure.json

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.mkuthan.streamprocessing.shared.test.common.JodaTimeArbitrary

final class JsonSerdeTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks with JodaTimeArbitrary {

  import JsonSerde._

  behavior of "JsonSerde"

  it should "serialize and deserialize" in {
    implicit val sampleClassArbitrary = Arbitrary[SampleClass] {
      for {
        string <- Gen.alphaNumStr
        optionString <- Gen.option(Gen.alphaNumStr)
        int <- Arbitrary.arbitrary[Int]
        double <- Arbitrary.arbitrary[Double]
        bigInt <- Arbitrary.arbitrary[BigInt]
        bigDecimal <- Arbitrary.arbitrary[BigDecimal]
        dateTime <- Arbitrary.arbitrary[DateTime]
        instant <- Arbitrary.arbitrary[Instant]
        localDate <- Arbitrary.arbitrary[LocalDate]
        localTime <- Arbitrary.arbitrary[LocalTime]
      } yield SampleClass(
        string,
        optionString,
        int,
        double,
        bigInt,
        bigDecimal,
        dateTime,
        instant,
        localDate,
        localTime
      )
    }

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

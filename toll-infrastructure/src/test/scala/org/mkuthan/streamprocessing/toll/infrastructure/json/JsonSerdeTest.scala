package org.mkuthan.streamprocessing.toll.infrastructure.json

import com.fortysevendeg.scalacheck.datetime.joda.ArbitraryJoda._
import com.fortysevendeg.scalacheck.datetime.YearRange
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class JsonSerdeTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  import JsonSerde._

  final case class Sample(
      string: String,
      int: Int,
      double: Double,
      bigDecimal: BigDecimal,
      dateTime: DateTime,
      instant: Instant,
      localDateTime: LocalDateTime,
      localDate: LocalDate,
      localTime: LocalTime
  )

  implicit val timeZone = DateTimeZone.UTC
  implicit val yearRange = YearRange.between(1900, 2100)

  implicit val instantArbitrary: Arbitrary[Instant] = Arbitrary {
    Arbitrary.arbitrary[DateTime].map(_.toInstant)
  }

  implicit val localDateTimeArbitrary: Arbitrary[LocalDateTime] = Arbitrary {
    Arbitrary.arbitrary[DateTime].map(_.toLocalDateTime)
  }

  implicit val localDateArbitrary: Arbitrary[LocalDate] = Arbitrary {
    Arbitrary.arbitrary[DateTime].map(_.toLocalDate)
  }

  implicit val localTimeArbitrary: Arbitrary[LocalTime] = Arbitrary {
    Arbitrary.arbitrary[DateTime].map(_.toLocalTime)
  }

  implicit val sampleClassArbitrary = Arbitrary[Sample] {
    for {
      string <- Gen.alphaNumStr
      int <- Arbitrary.arbitrary[Int]
      double <- Arbitrary.arbitrary[Double]
      bigDecimal <- Arbitrary.arbitrary[BigDecimal]
      dateTime <- Arbitrary.arbitrary[DateTime]
      instant <- Arbitrary.arbitrary[Instant]
      localDateTime <- Arbitrary.arbitrary[LocalDateTime]
      localDate <- Arbitrary.arbitrary[LocalDate]
      localTime <- Arbitrary.arbitrary[LocalTime]
    } yield Sample(
      string,
      int,
      double,
      bigDecimal,
      dateTime,
      instant,
      localDateTime,
      localDate,
      localTime
    )
  }

  behavior of "JsonSerde"

  it should "serialize and deserialize" in {
    forAll { sample: Sample =>
      val serialized = writeJsonAsString(sample)
      val deserialized = readJsonFromString[Sample](serialized).success.value
      deserialized shouldMatchTo (sample)
    }
  }

  it should "not deserialize unknown object" in {
    val unknownObjectJson = """{"unknownField":"a"}"""
    val result = readJsonFromString[Sample](unknownObjectJson)
    result.failure.exception should have message "No usable value for stringField\nDid not find value which can be converted into java.lang.String"
  }

}

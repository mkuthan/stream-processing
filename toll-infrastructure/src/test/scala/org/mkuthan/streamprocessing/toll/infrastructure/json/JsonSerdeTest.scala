package org.mkuthan.streamprocessing.toll.infrastructure.json

import com.fortysevendeg.scalacheck.datetime.instances.joda._
import com.fortysevendeg.scalacheck.datetime.GenDateTime._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.Period
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class JsonSerdeTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  import JsonSerde._
  import JsonSerdeTest._

  behavior of "JsonSerde"

  it should "serialize sample object" in {
    writeJsonAsString(SampleObject) should be(SampleJson)
  }

  it should "deserialize sample object" in {
    val result = readJsonFromString[Sample](SampleJson)
    result.success.value should be(SampleObject)
  }

  it should "not deserialize unknown object" in {
    val unknownObjectJson = """{"unknownField":"a"}"""
    val result = readJsonFromString[Sample](unknownObjectJson)
    result.failure.exception should have message "No usable value for stringField\nDid not find value which can be converted into java.lang.String"
  }

  it should "serialize and deserialize" in {
    val from: DateTime = DateTime.now(DateTimeZone.UTC)
    val range: Period = Period.years(100)

    implicit val sampleArb: Arbitrary[Sample] = Arbitrary {
      for {
        stringField <- Gen.alphaStr
        intField <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
        doubleField <- Gen.chooseNum(-100.0, 100.0)
        bigDecimalField <- Gen.choose(BigDecimal(-100), BigDecimal(100))
        dateTime <- genDateTimeWithinRange(from, range)
      } yield Sample(
        stringField,
        intField,
        doubleField,
        bigDecimalField,
        // TODO: it looks that Joda serializer doesn't support millis
        // https://github.com/json4s/json4s/issues/277
        dateTime.withMillis(0).toInstant(),
        dateTime.withMillis(0)
      )
    }

    forAll { sample: Sample =>
      val serialized = writeJsonAsString(sample)
      val deserialized = readJsonFromString[Sample](serialized).success.value
      deserialized should be(sample)
    }
  }
}

object JsonSerdeTest {
  final case class Sample(
      stringField: String,
      intField: Int,
      doubleField: Double,
      bigDecimalField: BigDecimal,
      instantField: Instant,
      dateTime: DateTime
  )

  private val SampleObject = Sample(
    "a",
    0,
    0.0,
    BigDecimal(0),
    Instant.EPOCH,
    Instant.EPOCH.toDateTime(DateTimeZone.UTC)
  )

  private val SampleJson = """{
    |"stringField":"a",
    |"intField":0,
    |"doubleField":0.0,
    |"bigDecimalField":"0",
    |"instantField":0,
    |"dateTime":"1970-01-01T00:00:00Z"
    |}""".stripMargin.replaceAll("\\n", "")
}

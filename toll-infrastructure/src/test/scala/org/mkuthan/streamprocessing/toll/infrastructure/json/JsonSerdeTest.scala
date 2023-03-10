package org.mkuthan.streamprocessing.toll.infrastructure.json

import org.joda.time.Instant
import org.joda.time.LocalDate
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
    implicit val sampleArb: Arbitrary[Sample] = Arbitrary {
      for {
        stringField <- Gen.alphaStr
        intField <- Gen.chooseNum(-100, 100)
        doubleField <- Gen.chooseNum(-100.0, 100.0)
        bigDecimalField <- Gen.choose(BigDecimal(-100), BigDecimal(100))
        millis <- Gen.posNum[Long]
      } yield Sample(
        stringField,
        intField,
        doubleField,
        bigDecimalField,
        Instant.ofEpochMilli(millis),
        Instant.ofEpochMilli(millis).toDateTime().toLocalDate()
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
      localDateField: LocalDate
  )

  private val SampleObject = Sample(
    "a",
    0,
    0.0,
    BigDecimal(0),
    Instant.EPOCH,
    LocalDate.parse("1970-01-01")
  )

  private val SampleJson = """{
    |"stringField":"a",
    |"intField":0,
    |"doubleField":0.0,
    |"bigDecimalField":"0",
    |"instantField":0,
    |"localDateField":{"year":1970,"month":1,"day":1}
    |}""".stripMargin.replaceAll("\\n", "")
}

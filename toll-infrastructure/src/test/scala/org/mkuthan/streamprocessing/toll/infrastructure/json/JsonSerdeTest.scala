package org.mkuthan.streamprocessing.toll.infrastructure.json

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.TryValues._

final class JsonSerdeTest extends AnyFlatSpec with Matchers {

  import JsonSerdeTest._

  private val anySampleObject = Sample("a", 0, Instant.EPOCH)
  private val anySampleJson = """{"f1":"a","f2":0,"f3":0}"""

  behavior of "JsonSerde"

  it should "serialize sample object" in {
    JsonSerde.writeJsonAsString(anySampleObject) should be(anySampleJson)
  }

  it should "deserialize sample object" in {
    val result = JsonSerde.readJsonFromString[Sample](anySampleJson)
    result.success.value should be(anySampleObject)
  }

  it should "not deserialize unknown object" in {
    val unknownObjectJson = """{"unknownField":"a"}"""
    val result = JsonSerde.readJsonFromString[Sample](unknownObjectJson)
    result.failure.exception should have message "No usable value for f1\nDid not find value which can be converted into java.lang.String"
  }
}

object JsonSerdeTest {
  // define all types used in domain
  final case class Sample(f1: String, f2: Int, f3: Instant)
}

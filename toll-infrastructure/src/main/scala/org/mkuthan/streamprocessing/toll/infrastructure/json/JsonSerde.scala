package org.mkuthan.streamprocessing.toll.infrastructure.json

import java.nio.charset.StandardCharsets

import scala.util.Try

import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.json4s.Formats

object JsonSerde {

  implicit val JsonFormats: Formats =
    DefaultFormats
      .lossless
      .withBigInt
      .withBigDecimal ++
      JodaTimeSerializers.all

  def writeJsonAsString[T <: AnyRef](obj: T): String =
    Serialization.write(obj)

  def writeJsonAsBytes[T <: AnyRef](obj: T): Array[Byte] =
    writeJsonAsString(obj).getBytes(StandardCharsets.UTF_8)

  def readJsonFromString[T <: AnyRef: Manifest](json: String): Try[T] =
    Try(Serialization.read[T](json))

  def readJsonFromBytes[T <: AnyRef: Manifest](json: Array[Byte]): Try[T] =
    readJsonFromString(new String(json, StandardCharsets.UTF_8))

}

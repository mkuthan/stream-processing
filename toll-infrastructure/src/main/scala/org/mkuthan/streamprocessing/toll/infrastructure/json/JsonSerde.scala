package org.mkuthan.streamprocessing.toll.infrastructure.json

import java.nio.charset.StandardCharsets

import scala.util.Try

import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.json4s.Formats

object JsonSerde {

  implicit val JsonFormats: Formats = DefaultFormats ++ JodaTimeSerializers.all

  def writeJsonAsString[T <: AnyRef](obj: T): String =
    Serialization.write(obj)

  def writeJsonAsBytes[T <: AnyRef](obj: T): Array[Byte] =
    writeJsonAsString(obj).getBytes(StandardCharsets.UTF_8)

  def readJsonFromString[T <: AnyRef](json: String)(implicit m: Manifest[T]): Try[T] =
    Try(Serialization.read[T](json))

  def readJsonFromBytes[T <: AnyRef](json: Array[Byte])(implicit m: Manifest[T]): Try[T] =
    readJsonFromString(new String(json, StandardCharsets.UTF_8))

}

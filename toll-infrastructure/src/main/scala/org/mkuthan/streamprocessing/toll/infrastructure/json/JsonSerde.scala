package org.mkuthan.streamprocessing.toll.infrastructure.json

import java.nio.charset.StandardCharsets

import scala.util.Try

import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.CustomSerializer
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.JString

object JsonSerde {

  implicit val JsonFormats: Formats = DefaultFormats.lossless ++ JodaTimeSerializers.all ++ customFormats

  def writeJsonAsString[T <: AnyRef](obj: T): String =
    Serialization.write(obj)

  def writeJsonAsBytes[T <: AnyRef](obj: T): Array[Byte] =
    writeJsonAsString(obj).getBytes(StandardCharsets.UTF_8)

  def readJsonFromString[T <: AnyRef](json: String)(implicit m: Manifest[T]): Try[T] =
    Try(Serialization.read[T](json))

  def readJsonFromBytes[T <: AnyRef](json: Array[Byte])(implicit m: Manifest[T]): Try[T] =
    readJsonFromString(new String(json, StandardCharsets.UTF_8))

  private def customFormats =
    List(
      // Use string instead of number to keep precision
      new CustomSerializer[BigDecimal](format =>
        (
          {
            case JString(x) => BigDecimal(x)
          },
          {
            case x: BigDecimal => JString(x.toString)
          }
        )
      )
    )

}

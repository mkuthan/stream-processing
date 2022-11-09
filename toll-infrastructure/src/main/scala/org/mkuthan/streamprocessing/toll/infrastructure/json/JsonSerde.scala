package org.mkuthan.streamprocessing.toll.infrastructure.json

import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.json4s.Formats

object JsonSerde {

  implicit val JsonFormats: Formats = DefaultFormats ++ JodaTimeSerializers.all

  def writeJson[T <: AnyRef](obj: T): String =
    Serialization.write(obj)

  def readJson[T <: AnyRef](json: String)(implicit mf: Manifest[T]): T =
    Serialization.read[T](json)

}

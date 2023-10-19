package org.mkuthan.streamprocessing.infrastructure.json

import scala.util.Try

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable

object JsonSerde {

  private lazy val ObjectMapper = new ObjectMapper()
    .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
    .registerModule(new JodaModule())
    .registerModule(DefaultScalaModule) :: ClassTagExtensions

  def writeJsonAsString[T <: AnyRef](obj: T): String =
    ObjectMapper.writeValueAsString(obj)

  def writeJsonAsBytes[T <: AnyRef](obj: T): Array[Byte] =
    ObjectMapper.writeValueAsBytes(obj)

  def readJsonFromString[T <: AnyRef: JavaTypeable](json: String): Try[T] =
    Try(ObjectMapper.readValue(json))

  def readJsonFromBytes[T <: AnyRef: JavaTypeable](json: Array[Byte]): Try[T] =
    Try(ObjectMapper.readValue(json))

}

package org.mkuthan.streamprocessing.toll.infrastructure.json

import scala.reflect.classTag
import scala.reflect.ClassTag
import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonSerde {

  private lazy val ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JodaModule())

  def writeJsonAsString[T <: AnyRef](obj: T): String =
    ObjectMapper.writeValueAsString(obj)

  def writeJsonAsBytes[T <: AnyRef](obj: T): Array[Byte] =
    ObjectMapper.writeValueAsBytes(obj)

  def readJsonFromString[T <: AnyRef: ClassTag](json: String): Try[T] =
    Try(ObjectMapper.readValue(json, classTag[T].runtimeClass.asInstanceOf[Class[T]]))

  def readJsonFromBytes[T <: AnyRef: ClassTag](json: Array[Byte]): Try[T] =
    Try(ObjectMapper.readValue(json, classTag[T].runtimeClass.asInstanceOf[Class[T]]))

}

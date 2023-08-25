package org.mkuthan.streamprocessing.infrastructure.pubsub

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._

private[pubsub] trait Utils {
  def readAttributes(attributes: JMap[String, String]): Map[String, String] =
    if (attributes == null) {
      Map.empty[String, String]
    } else {
      attributes.asScala.toMap
    }

  def writeAttributes(attributes: Map[String, String]): JMap[String, String] =
    if (attributes == null) {
      Map.empty[String, String].asJava
    } else {
      attributes.asJava
    }
}
package org.mkuthan.streamprocessing.test.common

import java.util.UUID

object RandomString {
  def randomString(): String =
    UUID.randomUUID.toString

  def randomStringUnderscored(): String =
    randomString().replace('-', '_')
}

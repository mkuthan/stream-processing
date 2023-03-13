package org.mkuthan.streamprocessing.shared.it.common

import java.util.UUID

object RandomString {
  def randomString(): String =
    UUID.randomUUID.toString

  def randomStringUnderscored(): String =
    randomString().replace('-', '_')
}

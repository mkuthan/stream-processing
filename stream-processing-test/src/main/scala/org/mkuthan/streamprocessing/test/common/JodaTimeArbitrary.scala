package org.mkuthan.streamprocessing.test.common

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait JodaTimeArbitrary {
  import JodaTimeArbitrary._

  implicit val instantArbitrary: Arbitrary[Instant] = Arbitrary[Instant] {
    for {
      instant <- Gen.chooseNum(Min.toInstant.getMillis, Max.toInstant.getMillis)
    } yield new Instant(instant)
  }

  implicit val dateTimeArbitrary: Arbitrary[DateTime] = Arbitrary[DateTime] {
    // TODO: use more timezones
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new DateTime(instant, DateTimeZone.UTC)
  }

  implicit val localDateTimeArbitrary: Arbitrary[LocalDateTime] = Arbitrary[LocalDateTime] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalDateTime(instant)
  }

  implicit val localDateArbitrary: Arbitrary[LocalDate] = Arbitrary[LocalDate] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalDate(instant)
  }

  implicit val localTimeArbitrary: Arbitrary[LocalTime] = Arbitrary[LocalTime] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalTime(instant)
  }
}

object JodaTimeArbitrary {
  private val Min: DateTime = DateTime.parse("1900-01-01T00:00:00.000Z")
  private val Max: DateTime = DateTime.parse("2099-12-31T23:59:59.999Z")
}

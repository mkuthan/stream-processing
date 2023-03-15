package org.mkuthan.streamprocessing.shared.test.scalacheck

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.joda.time.LocalTime
import org.scalacheck._

trait JodaTimeArbitrary {
  import JodaTimeArbitrary._

  implicit val instantArbitrary = Arbitrary[Instant] {
    for {
      instant <- Gen.chooseNum(Min.toInstant().getMillis(), Max.toInstant().getMillis())
    } yield new Instant(instant)
  }

  implicit val dateTimeArbitrary = Arbitrary[DateTime] {
    // TODO: use more timezones
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new DateTime(instant, DateTimeZone.UTC)
  }

  implicit val localDateTimeArbitrary = Arbitrary[LocalDateTime] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalDateTime(instant)
  }

  implicit val localDateArbitrary = Arbitrary[LocalDate] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalDate(instant)
  }

  implicit val localTimeArbitrary = Arbitrary[LocalTime] {
    for {
      instant <- Arbitrary.arbitrary[Instant]
    } yield new LocalTime(instant)
  }
}

object JodaTimeArbitrary {
  val Min = DateTime.parse("1900-01-01T00:00:00.000Z")
  val Max = DateTime.parse("2099-12-31T23:59:59.999Z")
}

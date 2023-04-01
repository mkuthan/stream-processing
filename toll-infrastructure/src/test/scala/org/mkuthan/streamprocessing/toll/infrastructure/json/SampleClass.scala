package org.mkuthan.streamprocessing.toll.infrastructure.json

import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.LocalDate
import org.joda.time.LocalTime

case class SampleClass(
    string: String,
    optionString: Option[String],
    int: Int,
    double: Double,
    bigInt: BigInt,
    bigDecimal: BigDecimal,
    dateTime: DateTime,
    instant: Instant,
    localDate: LocalDate,
    localTime: LocalTime
)

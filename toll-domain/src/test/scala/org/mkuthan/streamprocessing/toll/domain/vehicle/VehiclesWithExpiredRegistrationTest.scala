package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture

class VehiclesWithExpiredRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with VehiclesWithExpiredRegistrationFixture
    with TollBoothEntryFixture
    with VehicleRegistrationFixture {

  import VehiclesWithExpiredRegistration._

  private val FiveMinutes = Duration.standardMinutes(5)

  behavior of "VehiclesWithExpiredRegistration"

  it should "calculate expired VehicleRegistration" in runWithScioContext { sc =>
    val boothEntries = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(anyTollBoothEntry.entryTime, anyTollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = boundedTestCollectionOf[VehicleRegistration]
      .addElementsAtTime(
        anyVehicleRegistrationHistory.registrationTime,
        anyVehicleRegistrationHistory.copy(expired = true)
      )
      .addElementsAtTime(
        anyVehicleRegistrationUpdate.registrationTime,
        anyVehicleRegistrationUpdate.copy(expired = true)
      )
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testBounded(boothEntries), sc.testBounded(vehicleRegistrations), FiveMinutes)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containInAnyOrderAtTime(
        "2014-09-10T12:04:59.999Z",
        Seq(
          anyVehicleWithExpiredRegistration(anyVehicleRegistrationHistory.id),
          anyVehicleWithExpiredRegistration(anyVehicleRegistrationUpdate.id)
        )
      )
    }

    diagnostics should beEmpty
  }

  it should "emit diagnostic for non-expired VehicleRegistration" in runWithScioContext { sc =>
    val boothEntries = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(anyTollBoothEntry.entryTime, anyTollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = boundedTestCollectionOf[VehicleRegistration]
      .addElementsAtTime(
        anyVehicleRegistrationHistory.registrationTime,
        anyVehicleRegistrationHistory.copy(expired = true)
      )
      .addElementsAtTime(
        anyVehicleRegistrationUpdate.registrationTime,
        anyVehicleRegistrationUpdate.copy(expired = false)
      )
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testBounded(boothEntries), sc.testBounded(vehicleRegistrations), FiveMinutes)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        anyVehicleWithExpiredRegistration(anyVehicleRegistrationHistory.id)
      )
    }

    diagnostics.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        vehicleWithNotExpiredRegistrationDiagnostic
      )
    }
  }

  it should "emit diagnostic for missing VehicleRegistration" in runWithScioContext { sc =>
    val unknownLicensePlate = LicensePlate("Unknown License Plate")

    val boothEntries = boundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(anyTollBoothEntry.entryTime, anyTollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = boundedTestCollectionOf[VehicleRegistration]
      .addElementsAtTime(
        anyVehicleRegistrationHistory.registrationTime,
        anyVehicleRegistrationHistory.copy(licensePlate = unknownLicensePlate)
      )
      .addElementsAtTime(
        anyVehicleRegistrationUpdate.registrationTime,
        anyVehicleRegistrationUpdate.copy(licensePlate = unknownLicensePlate)
      )
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testBounded(boothEntries), sc.testBounded(vehicleRegistrations), FiveMinutes)

    results should beEmpty

    diagnostics.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        vehicleWithMissingRegistrationDiagnostic
      )
    }
  }

  it should "encode into message" in runWithScioContext { sc =>
    val createdAt = Instant.parse("2014-09-10T12:09:59.999Z")

    val inputs = unboundedTestCollectionOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(createdAt, anyVehicleWithExpiredRegistration())
      .advanceWatermarkToInfinity()

    val results = encodeMessage(sc.testUnbounded(inputs))
    results should containSingleValue(anyVehicleWithExpiredRegistrationMessage(createdAt))
  }

  it should "encode into record" in runWithScioContext { sc =>
    val createdAt = Instant.parse("2014-09-10T23:59:59.999Z")

    val inputs = boundedTestCollectionOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(createdAt, anyVehicleWithExpiredRegistration())
      .advanceWatermarkToInfinity()

    val results = encodeRecord(sc.testBounded(inputs))
    results should containSingleValue(anyVehicleWithExpiredRegistrationRecord(createdAt))
  }
}

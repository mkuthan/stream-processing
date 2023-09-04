package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.common.Message
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
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val licencePlate = LicensePlate("License Plate 1")

    val tollBoothEntry = anyTollBoothEntry.copy(entryTime = entryTime, licensePlate = licencePlate)
    val vehicleRegistration = anyVehicleRegistration.copy(licensePlate = licencePlate, expired = true)

    val boothEntries = unboundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(entryTime, tollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = unboundedTestCollectionOf[VehicleRegistration]
      .addElementsAtWatermarkTime(vehicleRegistration)
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testUnbounded(boothEntries), sc.testUnbounded(vehicleRegistrations), FiveMinutes)

    results.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        anyVehicleWithExpiredRegistration.copy(entryTime = entryTime, licensePlate = licencePlate)
      )
    }

    diagnostics should beEmpty
  }

  it should "emit diagnostic for non-expired VehicleRegistration" in runWithScioContext { sc =>
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val licencePlate = LicensePlate("License Plate 1")

    val tollBoothEntry = anyTollBoothEntry.copy(entryTime = entryTime, licensePlate = licencePlate)
    val vehicleRegistration = anyVehicleRegistration.copy(licensePlate = licencePlate, expired = false)

    val boothEntries = unboundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(entryTime, tollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = unboundedTestCollectionOf[VehicleRegistration]
      .addElementsAtWatermarkTime(vehicleRegistration)
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testUnbounded(boothEntries), sc.testUnbounded(vehicleRegistrations), FiveMinutes)

    results should beEmpty

    diagnostics.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        vehicleWithNotExpiredRegistrationDiagnostic
      )
    }
  }

  it should "emit diagnostic for missing VehicleRegistration" in runWithScioContext { sc =>
    val entryTime = Instant.parse("2014-09-10T12:03:01Z")
    val tollBoothEntry = anyTollBoothEntry.copy(entryTime = entryTime, licensePlate = LicensePlate("License Plate 1"))
    val vehicleRegistration = anyVehicleRegistration.copy(licensePlate = LicensePlate("License Plate 2"))

    val boothEntries = unboundedTestCollectionOf[TollBoothEntry]
      .addElementsAtTime(entryTime, tollBoothEntry)
      .advanceWatermarkToInfinity()

    val vehicleRegistrations = unboundedTestCollectionOf[VehicleRegistration]
      .addElementsAtWatermarkTime(vehicleRegistration)
      .advanceWatermarkToInfinity()

    val (results, diagnostics) =
      calculateInFixedWindow(sc.testUnbounded(boothEntries), sc.testUnbounded(vehicleRegistrations), FiveMinutes)

    results should beEmpty

    diagnostics.withTimestamp should inOnTimePane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containSingleValueAtTime(
        "2014-09-10T12:04:59.999Z",
        vehicleWithMissingRegistrationDiagnostic
      )
    }
  }

  it should "encode into Raw" in runWithScioContext { sc =>
    val inputs = unboundedTestCollectionOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(anyVehicleWithExpiredRegistrationRaw.created_at, anyVehicleWithExpiredRegistration)
      .advanceWatermarkToInfinity()

    val results = encode(sc.testUnbounded(inputs))
    results should containSingleValue(Message(anyVehicleWithExpiredRegistrationRaw))
  }
}

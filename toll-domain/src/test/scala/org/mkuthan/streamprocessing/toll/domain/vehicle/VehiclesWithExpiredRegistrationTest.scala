package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.TestScioContext
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnosticFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture

class VehiclesWithExpiredRegistrationTest extends AnyFlatSpec with Matchers
    with TestScioContext
    with TollBoothEntryFixture
    with TollBoothDiagnosticFixture
    with VehicleRegistrationFixture
    with VehiclesWithExpiredRegistrationFixture {

  import VehiclesWithExpiredRegistration._

  private val FiveMinutes = Duration.standardMinutes(5)
  private val TwoDays = Duration.standardDays(2)

  private val DefaultWindowOptions = WindowOptions()

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
      calculateWithTemporalJoin(
        sc.testBounded(boothEntries),
        sc.testBounded(vehicleRegistrations),
        FiveMinutes,
        TwoDays,
        DefaultWindowOptions
      )

    results.withTimestamp should inOnlyPane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containElementsAtTime(
        anyTollBoothEntry.entryTime,
        anyVehicleWithExpiredRegistration(anyVehicleRegistrationHistory.id),
        anyVehicleWithExpiredRegistration(anyVehicleRegistrationUpdate.id)
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
      calculateWithTemporalJoin(
        sc.testBounded(boothEntries),
        sc.testBounded(vehicleRegistrations),
        FiveMinutes,
        TwoDays,
        DefaultWindowOptions
      )

    results.withTimestamp should inOnlyPane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containElementsAtTime(
        anyTollBoothEntry.entryTime,
        anyVehicleWithExpiredRegistration(anyVehicleRegistrationHistory.id)
      )
    }

    diagnostics.withTimestamp should inOnlyPane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containElementsAtTime(
        anyTollBoothEntry.entryTime,
        anyTollBoothDiagnostic.copy(reason = TollBoothDiagnostic.VehicleRegistrationNotExpired)
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
      calculateWithTemporalJoin(
        sc.testBounded(boothEntries),
        sc.testBounded(vehicleRegistrations),
        FiveMinutes,
        TwoDays,
        DefaultWindowOptions
      )

    results should beEmpty

    diagnostics.withTimestamp should inOnlyPane("2014-09-10T12:00:00Z", "2014-09-10T12:05:00Z") {
      containElementsAtTime(
        anyTollBoothEntry.entryTime,
        anyTollBoothDiagnostic.copy(reason = TollBoothDiagnostic.MissingVehicleRegistration)
      )
    }
  }

  it should "encode into message" in runWithScioContext { sc =>
    val createdAt = Instant.parse("2014-09-10T12:09:59.999Z")

    val inputs = unboundedTestCollectionOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(createdAt, anyVehicleWithExpiredRegistration())
      .advanceWatermarkToInfinity()

    val results = encodeMessage(sc.testUnbounded(inputs))
    results should containElements(anyVehicleWithExpiredRegistrationMessage(createdAt))
  }

  it should "encode into record" in runWithScioContext { sc =>
    val createdAt = Instant.parse("2014-09-10T23:59:59.999Z")

    val inputs = boundedTestCollectionOf[VehiclesWithExpiredRegistration]
      .addElementsAtTime(createdAt, anyVehicleWithExpiredRegistration())
      .advanceWatermarkToInfinity()

    val results = encodeRecord(sc.testBounded(inputs))
    results should containElements(anyVehicleWithExpiredRegistrationRecord(createdAt))
  }
}

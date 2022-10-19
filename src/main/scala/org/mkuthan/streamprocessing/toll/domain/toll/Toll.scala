package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.values.SCollection
import org.mkuthan.streamprocessing.toll.domain.booth.{TollBoothEntry, TollBoothExit}
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration

object Toll {
  def totalTimeForEachCar(
      boothEntries: SCollection[TollBoothEntry],
      boothExits: SCollection[TollBoothExit]
  ): (SCollection[TotalCarTime], SCollection[Diagnostic]) = ???

  def vehiclesWithExpiredRegistration(
      boothEntries: SCollection[TollBoothEntry],
      vehicleRegistration: SCollection[VehicleRegistration]
  ): (SCollection[VehiclesWithExpiredRegistration], SCollection[Diagnostic]) = ???
}

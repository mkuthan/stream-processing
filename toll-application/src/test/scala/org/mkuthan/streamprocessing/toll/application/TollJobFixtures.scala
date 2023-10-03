package org.mkuthan.streamprocessing.toll.application

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStatsFixture
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimesFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationFixture

trait TollJobFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothStatsFixture
    with TotalVehicleTimesFixture
    with VehicleRegistrationFixture
    with VehiclesWithExpiredRegistrationFixture

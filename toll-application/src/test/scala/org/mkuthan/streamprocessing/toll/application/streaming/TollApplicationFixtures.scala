package org.mkuthan.streamprocessing.toll.application.streaming

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStatsFixture
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeFixture
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationFixture

trait TollApplicationFixtures
    extends TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothStatsFixture
    with TotalVehicleTimeFixture
    with VehicleRegistrationFixture
    with VehiclesWithExpiredRegistrationFixture

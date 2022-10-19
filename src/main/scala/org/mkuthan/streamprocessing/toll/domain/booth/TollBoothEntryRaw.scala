package org.mkuthan.streamprocessing.toll.domain.booth

case class TollBoothEntryRaw(
    id: String,
    entry_time: String,
    license_plate: String,
    state: String,
    make: String,
    model: String,
    vehicle_type: String,
    weight_type: String,
    toll: String,
    tag: String
)

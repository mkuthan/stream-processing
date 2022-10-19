package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.toTable
case class VehicleRegistrationRaw(
    id: String,
    licence_plate: String,
    expired: Int
)

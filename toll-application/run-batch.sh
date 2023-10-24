#!/bin/bash

DATE=`date +%Y-%m-%d`

PROJECT=playground-272019
REGION=europe-central2

gcloud dataflow flex-template run "toll-application-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://$PROJECT-toll-application/templates/toll-application-batch.json" \
    --region "$REGION" \
    --staging-location "gs://$PROJECT-toll-application/staging/" \
    --additional-experiments "enable_prime" \
    --parameters effectiveDate="$DATE" \
    --parameters entryTable="$PROJECT.toll_application.toll-booth-entry" \
    --parameters exitTable="$PROJECT.toll_application.toll-booth-exit" \
    --parameters vehicleRegistrationTable="$PROJECT.toll_application.vehicle-registration" \
    --parameters entryStatsHourlyTable="$PROJECT.toll_application.toll-booth-entry-stats-hourly" \
    --parameters entryStatsDailyTable="$PROJECT.toll_application.toll-booth-entry-stats-daily" \
    --parameters totalVehicleTimesOneHourGapTable="$PROJECT.toll_application.total-vehicle-times-one-hour-gap" \
    --parameters totalVehicleTimesDiagnosticOneHourGapTable="$PROJECT.toll_application.total-vehicle-times-diagnostic-one-hour-gap" \
    --parameters vehiclesWithExpiredRegistrationDailyTable="$PROJECT.toll_application.vehicles-with-expired-registration-daily" \
    --parameters vehiclesWithExpiredRegistrationDiagnosticDailyTable="$PROJECT.toll_application.vehicles-with-expired-registration-diagnostic-daily" \
   
    # --additional-experiments "use_runner_v2" \
    # --max-workers 1 \
    # --worker-machine-type "t2d-standard-1" \

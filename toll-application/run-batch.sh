#!/bin/bash

DATE=`date +%Y-%m-%d`

PROJECT=playground-272019
REGION=europe-west1

gcloud dataflow flex-template run "toll-application-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://$PROJECT-toll-application/templates/toll-application-batch.json" \
    --region "$REGION" \
    --staging-location "gs://$PROJECT-toll-application/staging/" \
    --additional-experiments "use_runner_v2" \
    --max-workers 1 \
    --worker-machine-type "t2d-standard-1" \
    --parameters effectiveDate="$DATE" \
    --parameters entryTable="$PROJECT.toll_application.toll-booth-entry" \
    --parameters exitTable="$PROJECT.toll_application.toll-booth-exit" \
    --parameters vehicleRegistrationTable="$PROJECT.toll_application.vehicle-registration" \
    --parameters entryStatsHourlyTable="$PROJECT.toll_application.toll-booth-entry-stats-hourly" \
    --parameters entryStatsDailyTable="$PROJECT.toll_application.toll-booth-entry-stats-daily" \
    --parameters totalVehicleTimesOneHourGapTable="$PROJECT.toll_application.total-vehicle-times-one-hour-gap" \
    --parameters totalVehicleTimesOneHourGapDiagnosticTable="$PROJECT.toll_application.total-vehicle-times-one-hour-gap-diagnostic" \
    --parameters vehiclesWithExpiredRegistrationDailyTable="$PROJECT.toll_application.vehicles-with-expired-registration-daily" \
    --parameters vehiclesWithExpiredRegistrationDailyDiagnosticTable="$PROJECT.toll_application.vehicles-with-expired-registration-daily-diagnostic" \

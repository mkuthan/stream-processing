#!/bin/bash

PROJECT=playground-272019
REGION=europe-central2

gcloud dataflow flex-template run "toll-application-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://$PROJECT-toll-application/templates/toll-application-streaming.json" \
    --region "$REGION" \
    --staging-location "gs://$PROJECT-toll-application/staging/" \
    --enable-streaming-engine \
    --additional-experiments "use_runner_v2" \
    --worker-machine-type "t2d-standard-1" \
    --max-workers 2 \
    --parameters entrySubscription="projects/$PROJECT/subscriptions/toll-booth-entry" \
    --parameters entryDlq="gs://$PROJECT-toll-application/dlq/entry" \
    --parameters exitSubscription="projects/$PROJECT/subscriptions/toll-booth-exit" \
    --parameters exitDlq="gs://$PROJECT-toll-application/dlq/exit" \
    --parameters vehicleRegistrationSubscription="projects/$PROJECT/subscriptions/vehicle-registration" \
    --parameters vehicleRegistrationDlq="gs://$PROJECT-toll-application/dlq/vehicle-registration" \
    --parameters vehicleRegistrationTable="$PROJECT.toll_application.vehicle-registration" \
    --parameters entryStatsTable="$PROJECT.toll_application.toll-booth-entry-stats" \
    --parameters totalVehicleTimesTable="$PROJECT.toll_application.total-vehicle-times" \
    --parameters totalVehicleTimesDiagnosticTable="$PROJECT.toll_application.total-vehicle-times-diagnostic" \
    --parameters vehiclesWithExpiredRegistrationTopic="projects/$PROJECT/topics/vehicle-registration" \
    --parameters vehiclesWithExpiredRegistrationTable="$PROJECT.toll_application.vehicles-with-expired-registration" \
    --parameters vehiclesWithExpiredRegistrationDiagnosticTable="$PROJECT.toll_application.vehicles-with-expired-registration-diagnostic" \
    --parameters ioDiagnosticTable="$PROJECT.toll_application.io-diagnostic" \

#    --additional-experiments "enable_prime" \

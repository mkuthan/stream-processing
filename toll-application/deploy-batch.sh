#!/bin/bash

PROJECT=playground-272019
REGION=europe-west1

gcloud dataflow flex-template build gs://$PROJECT-toll-application/templates/toll-application-batch.json \
    --image-gcr-path "$REGION-docker.pkg.dev/$PROJECT/toll-application/toll-application-batch:latest" \
    --sdk-language "JAVA" \
    --flex-template-base-image JAVA17 \
    --jar "target/scala-2.13/toll-application.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.mkuthan.streamprocessing.toll.application.batch.TollBatchJob" \

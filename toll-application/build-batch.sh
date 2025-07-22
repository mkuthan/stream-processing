#!/bin/bash

TEMPLATE_FILE=gs://playground-272019-toll-application/templates/toll-application-batch.json
IMAGE_GCR=europe-west1-docker.pkg.dev/playground-272019/toll-application/toll-application-batch:latest
JAR=./target/scala-2.13/assembly.jar
MAIN_CLASS=org.mkuthan.streamprocessing.toll.application.batch.TollBatchJob

(cd .. && sbt assembly)

gcloud dataflow flex-template build $TEMPLATE_FILE \
            --image-gcr-path $IMAGE_GCR \
            --sdk-language JAVA \
            --flex-template-base-image JAVA17 \
            --jar $JAR \
            --env FLEX_TEMPLATE_JAVA_MAIN_CLASS=$MAIN_CLASS

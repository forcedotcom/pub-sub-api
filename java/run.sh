#!/bin/sh
#
# A convenience script that runs examples based on locally built JARs. Usage:
#   mvn clean package
#   ./run.sh <package-name><class-name>
#

EXAMPLE=$1
if [ "x$EXAMPLE" = "x" ]; then
  echo "Please specify one of the example class names from the package com.salesforce.eventbusclient.example"
  exit -1
fi
java -cp target/pubsub-java-1.0-SNAPSHOT-jar-with-dependencies.jar $EXAMPLE $@
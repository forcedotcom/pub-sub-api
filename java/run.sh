#!/bin/sh
#
# A convenience script that runs examples based on locally built JARs. Usage:
#   mvn clean package
#   ./run.sh Publish -u <username> -p <pwd> -l <loginurl>
#   optional parameters:
#   -grpcHost <host>: defaults to localhost
#   -grpcPort <port number>: defaults to 7011
#   -t <topic>: defaults to /event/CarMaintenance__e
#   -n <number of events to publish>: defaults to 5 and used only in genericpubsub.PublishStream example
#

EXAMPLE=$1
if [ "x$EXAMPLE" = "x" ]; then
  echo "Please specify one of the example class names from the package com.salesforce.eventbusclient.example"
  exit -1
fi
java -cp target/pubsub-java-1.0-SNAPSHOT-jar-with-dependencies.jar $EXAMPLE $@
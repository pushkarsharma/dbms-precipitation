#!/bin/bash

mvn clean install
spark-submit --driver-memory 10g target/Precipitation-0.0.1-SNAPSHOT-jar-with-dependencies.jar $1 $2 $3
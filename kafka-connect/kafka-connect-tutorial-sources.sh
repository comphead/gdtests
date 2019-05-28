#!/bin/bash

# Make sure you change the ADV_HOST variable in docker-compose.yml
# if you are using docker Toolbox

# 1) Source connectors
# Start our kafka cluster
docker-compose up kafka-cluster
# Wait 2 minutes for the kafka cluster to be started

###############
# A) FileStreamSourceConnector in standalone mode
# Look at the source/demo-1/worker.properties file and edit bootstrap
# Look at the source/demo-1/file-stream-demo.properties file
# Look at the demo-file.txt file

# We start a hosted tools, mapped on our code
# Linux / Mac
docker run --rm -it -v "$(pwd)":/tutorial --net=host landoop/fast-data-dev:latest bash

# we launch the kafka connector in standalone mode:
cd /tutorial/source/demo-1
# create the topic we write to with 3 partitions
kafka-topics --create --topic bot.source --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
# Usage is connect-standalone worker.properties connector1.properties [connector2.properties connector3.properties]
connect-standalone worker.properties file-stream-demo-standalone.properties
# write some data to the demo-file.txt !
# shut down the terminal when you're done.
###############

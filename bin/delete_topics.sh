#!/bin/bash

kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic testks.orders  --command-config ./cfg/client.properties
kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic testks.customers   --command-config ./cfg/client.properties
kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic orders-joiner-v1-customers-changelog  --command-config ./cfg/client.properties


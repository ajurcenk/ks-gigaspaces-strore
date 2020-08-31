#!/bin/bash

kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.customers  --partitions 9 --replication-factor 3 --command-config ./cfg/client.properties
kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.orders     --partitions 9 --replication-factor 3 --command-config ./cfg/client.properties

kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --list --command-config ./../cfg/client.properties

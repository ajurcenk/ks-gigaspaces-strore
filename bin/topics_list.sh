#!/bin/bash

kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --list --command-config ./../cfg/client.properties

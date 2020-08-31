#!/bin/bash
export GS_LOOKUP_LOCATORS=10.0.2.15:4174
mvn exec:java -f ./../pom.xml -Dexec.mainClass=com.gigaspaces.demo.kstreams.app.App



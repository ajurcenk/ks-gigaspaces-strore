#!/bin/bash
export GS_HOME=/Users/aleksej/projects/gigaspaces/gigaspaces-xap-enterprise-15.2.0
$GS_HOME/bin/gs.sh space deploy --partitions=1 orders-joiner

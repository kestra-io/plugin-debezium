#!/bin/bash

# Start Cassandra in the background
cassandra -f &

# Wait for Cassandra to start
sleep 30

# Execute CQL script
cqlsh -f /etc/cassandra/cassandra.cql

# Wait for background jobs to finish
wait
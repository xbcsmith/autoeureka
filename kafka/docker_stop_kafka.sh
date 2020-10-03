#!/bin/bash

echo stopping Kafka and Zookeeper

docker rm -f zookeeper kafka

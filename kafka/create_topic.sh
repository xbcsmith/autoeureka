#!/bin/bash

NEWTOPIC="${1:=foo}"

/opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --topic ${NEWTOPIC} --partitions 3 --replication-factor 3

/opt/bitnami/kafka/bin/kafka-topics.sh --describe --zookeeper zookeeper:2181 --topic ${NEWTOPIC}

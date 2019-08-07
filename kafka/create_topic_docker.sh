#!/bin/bash

NEWTOPIC="${NEWTOPIC:=foo}"

docker run -it --network kafka_default bitnami/kafka:latest /opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --topic ${NEWTOPIC} --partitions 3 --replication-factor 3

docker run -it --network kafka_default bitnami/kafka:latest /opt/bitnami/kafka/bin/kafka-topics.sh --describe --zookeeper zookeeper:2181 --topic ${NEWTOPIC}

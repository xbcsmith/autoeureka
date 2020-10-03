#!/bin/bash

DOCKER_IP=127.0.0.1

echo starting Zookeeper...
docker run -d -t --name zookeeper \
	-p 2181:2181                       \
	wurstmeister/zookeeper

echo starting Kafka...
docker run -d -t --name kafka             \
	-e HOST_IP=kafka                           \
	-e KAFKA_ADVERTISED_HOST_NAME=${DOCKER_IP} \
	-e KAFKA_ADVERTISED_PORT=9092              \
	-e KAFKA_NUM_PARTITIONS=10                 \
	-e KAFKA_DEFAULT_REPLICATION_FACTOR=1      \
	-e KAFKA_REPLICATION_FACTOR=1              \
	-p ${DOCKER_IP}:9092:9092                  \
	-p ${DOCKER_IP}:9997:9997                  \
	-e KAFKA_BROKER_ID=1                       \
	-e ZK=zk -p 9092                           \
	--link zookeeper:zk                        \
	wurstmeister/kafka:0.10.1.0

# Kafka Experiment

docker-compose up

docker build -t kafkatools:latest -f Dockerfile .

## Run Tools Container

docker run -it --network kafka_default kafkatools:latest /bin/bash


## In Container

/usr/local/bin/create_topic.sh

kafkacat -C -b kafka1:9092,kafka2:9092,kafka3:9092 -t foo -p 0

echo '{"publish": true, "partition": "0"}' | kafkacat -P -b kafka1:9092,kafka2:9092,kafka3:9092 -t foo -p 0

/usr/local/bin/create_topic.sh shark

kafkacat -C -b kafka1:9092,kafka2:9092,kafka3:9092 -t shark -p 0

echo '{"publish": true, "partition": "0", "shark": "hammerhead"}' | kafkacat -P -b kafka1:9092,kafka2:9092,kafka3:9092 -t shark -p 0


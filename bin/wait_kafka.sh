#!/usr/bin/env bash

echo "Wait for all brokers to be up-and-running"
docker-compose logs zk1 | grep -iq "binding" && echo "zk1 up"
docker-compose logs -f kafka-1 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka-1 up"
docker-compose logs -f kafka-2 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka-2 up"
docker-compose logs -f kafka-3 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka-3 up"

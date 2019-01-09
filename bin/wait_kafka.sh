#!/usr/bin/env bash

echo "Wait for all brokers to be up-and-running"
docker-compose logs zk1 | grep -iq "binding" && echo "zk1 up"
docker-compose logs -f kafka1 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka1 up"
docker-compose logs -f kafka2 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka2 up"
docker-compose logs -f kafka3 | grep -q "started (kafka.server.KafkaServer)" && echo "kafka3 up"

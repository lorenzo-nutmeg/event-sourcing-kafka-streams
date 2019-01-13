# Event Sourcing with Kafka Streams

[![Build Status](https://travis-ci.org/nicusX/event-sourcing-kafka-streams.svg?branch=master)](https://travis-ci.org/nicusX/event-sourcing-kafka-streams)

It demonstrates how to use [Kafka Streams](https://kafka.apache.org/documentation/streams/)
for implementing an event sourced system.

### Credits

This project is a fork of [Amitay Horwitz's work](https://github.com/amitayh/event-sourcing-kafka-streams), 
presented at [JEEConf 2018](https://speakerdeck.com/amitayh/event-sourcing-with-kafka-streams) 
and [CodeMotion Milan 2018](https://speakerdeck.com/amitayh/building-event-sourced-systems-with-kafka-streams).

All credits go to [the author](https://github.com/amitayh).


## Running the project locally

The following instructions have been tested on OSX only, especially for the Docker part.

### Requirements

1. sbt ([help](https://www.scala-sbt.org/))
2. Docker

### Start Kafka and MySQL in Docker, locally

From `./docker` directory: 
   
   ```
   $ docker-compose up -d
   $ ../bin/wait_kafka.sh

   ```

### Build the project

1. Build the project:

   ```
   $ sbt assembly
   ```

2. Create the topics (edit `config/local.properties` as needed):

   ```
   $ bin/setup.sh config/local.properties
   ```

### Running

1. Run the [command handler](commandhandler/src/main/scala/org/amitayh/invoices/commandhandler/CommandHandler.scala):

   ```
   $ bin/commandhandler.sh config/local.properties
   ```

2. Run the [invoices list projector](listprojector/src/main/scala/org/amitayh/invoices/projector/ListProjector.scala):

   ```
   $ bin/projector.sh config/local.properties
   ```

3. Run the [web server](web/src/main/scala/org/amitayh/invoices/web/InvoicesServer.scala):

   ```
   $ bin/web.sh config/local.properties
   ```

### Stopping Kafka and MySQL

(from `./docker`) 

   ```
   $ docker-compose down
   ```

Tearing down the cluster properly clear its state (topics setup, contents)
   
### Demo application

If everything worked, you should be able to see the app running at `http://localhost:8080/index.html`

### Directly access Kafka and MySQL

To access Kafka and MySQL directly:

Kafka  (`PLAINTEXT`)
- `kafka-1`: `localhost:9092`
- `kafka-2`: `localhost:9093`
- `kafka-3`: `localhost:9094`

Zookeeper:
- `zk1`: `localhost:2181`

Schema Registry:
- `schema-registry`: `http://localhost:8081`

MySQL:
- `localhost:33306`  (`root`/`secret` or `mysqluser`/`secret`)



## License

Original Copyright © 2018 Amitay Horwitz

Additions by Lorenzo Nicora

Distributed under the MIT License


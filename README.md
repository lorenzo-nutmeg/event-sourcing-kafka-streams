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

### Access application and data   

If everything worked, you should be able to see the app running at `http://localhost:8080/index.html`

MySQL is accessible on localhost:33306 (root/secret)

### Stopping Kafka and MySQL

1. (from `./docker`) 

   ```
   $ docker-compose down
   ```

## License

Original Copyright © 2018 Amitay Horwitz

Additions by Lorenzo Nicora

Distributed under the MIT License


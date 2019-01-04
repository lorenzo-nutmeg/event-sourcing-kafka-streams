# Event Sourcing with Kafka Streams

This POC is based on [Amitay Horwitz's sample project](https://github.com/amitayh/event-sourcing-kafka-streams)
and demonstrate how to use [Kafka Streams](https://kafka.apache.org/documentation/streams/)
for implementing an event sourced system.


## Running the project locally

The following instructions have been tested on OSX only, especially for the Docker part.

### Requirements

1. sbt ([help](https://www.scala-sbt.org/))
2. Docker

### Start Kafka and MySQL in Docker, locally

1. Set `HOST_IP` env variable to the IP of the machine (not localhost!)
   
   On a MacBook you can use: 
   
   ```
   $ export HOST_IP=$(ifconfig | grep -A 1 'en0' | tail -1 | cut -d ' ' -f 2)`
   ```

2. Start  Kafka, Zookeeper, MySQL with Docker Compose and wait for the cluster to settle down:
   (from `./docker`) 
   
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

Distributed under the MIT License

Additions by Lorenzo Nicora
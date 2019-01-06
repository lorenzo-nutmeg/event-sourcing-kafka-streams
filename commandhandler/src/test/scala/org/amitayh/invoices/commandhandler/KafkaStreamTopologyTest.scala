package org.amitayh.invoices.commandhandler

import java.util.Properties

import org.amitayh.invoices.streamprocessor.TopologyDefinition
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}

// FIXME Move to stream processor (it is useful for testing other stream processors)
trait KafkaStreamTopologyTest {
  this: TopologyDefinition =>

  class InputTopic[K,V](topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
    val recordFactory = new ConsumerRecordFactory(keySerializer, valueSerializer)

    def pipeIn(key: K, value: V): Unit =
      driver.pipeInput(recordFactory.create(topicName, key, value))
  }

  class OutputTopic[K,V](topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {
    def pipeOut: Option[ProducerRecord[K,V]] = {
      Option(driver.readOutput(topicName, keyDeserializer, valueDeserializer))
    }
  }


  private val driver: TopologyTestDriver = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    new TopologyTestDriver(topology, props)
  }

  def createInputTopic[K,V](topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]): InputTopic[K,V] =
    new InputTopic(topicName, keySerializer, valueSerializer)

  def createOutputTopic[K,V](topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) =
    new OutputTopic(topicName, keyDeserializer, valueDeserializer)

  def testTopology(run: () => Unit): Unit = {
    try {
      run()
    } finally {
      // Don't forget to close the test driver!
      driver.close()
    }
  }

}
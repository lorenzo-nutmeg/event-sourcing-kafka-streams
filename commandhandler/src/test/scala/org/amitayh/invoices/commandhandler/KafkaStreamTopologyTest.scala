package org.amitayh.invoices.commandhandler

import java.util.Properties

import org.amitayh.invoices.streamprocessor.TopologyDefinition
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}


// FIXME Move to streamprocessor (it is useful for testing other stream processors)
trait KafkaStreamTopologyTest {
  this: TopologyDefinition =>

  var driver: TopologyTestDriver = _ // Must be re-initialised at every test

  class InputTopic[K,V](topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
    val recordFactory = new ConsumerRecordFactory(keySerializer, valueSerializer)
    def pipeIn(key: K, value: V): Unit =
        driver.pipeInput(recordFactory.create(topicName, key, value))
  }

  class OutputTopic[K,V](topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {
    def receive: Option[ProducerRecord[K,V]] = {
      Option(driver.readOutput(topicName, keyDeserializer, valueDeserializer))
    }
  }

  class KVStore[K,V](storeName: String) {
    val store: KeyValueStore[K,V] = driver.getKeyValueStore(storeName)
    def get(key: K): Option[V] = Option(store.get(key))
  }


  def inputTopic[K,V](topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]): InputTopic[K,V] =
    new InputTopic(topicName, keySerializer, valueSerializer)

  def outputTopic[K,V](topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) =
    new OutputTopic(topicName, keyDeserializer, valueDeserializer)

  def keyValueStore[K,V](storeName: String): KVStore[K,V] =
    new KVStore(storeName)

  def testTopology(run: () => Unit): Unit = {
    before
    try {
      run()
    } finally {
      after
    }
  }

  private def before: Unit = {
    driver = {
      val props = new Properties
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
      props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

      new TopologyTestDriver(topology(new StreamsBuilder).build, props)
    }
  }

  private def after: Unit = {
    driver.close
  }

}

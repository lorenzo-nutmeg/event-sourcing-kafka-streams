package org.amitayh.invoices.commandhandler

import java.util.Properties

import org.amitayh.invoices.streamprocessor.TopologyDefinition
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver => Driver}


trait TopologyTest {

  def topologyDefinitionUnderTest: TopologyDefinition

  // Run with default properties
  def test(run: ( () => Driver ) => Unit ): Unit = {
    test(new Properties)(run)
  }

  // Run with additional properties
  def test(props: Properties)(run: ( () => Driver ) => Unit ): Unit = {
    val driver = before(props)
    try {
      run( () => driver )
    } finally {
      after(driver)
    }
  }


  private def before(additionalProps: Properties) : Driver = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props.putAll(additionalProps)

    new Driver(topologyDefinitionUnderTest(new StreamsBuilder).build, props)
  }

  private def after(driver: Driver): Unit = {
    driver.close
  }



  case class InputTopic[K,V](topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
    private val recordFactory = new ConsumerRecordFactory(keySerializer, valueSerializer)

    def pipeIn(driver: () => Driver)(key: K, value: V): this.type = {
      driver().pipeInput(recordFactory.create(topicName, key, value))
      this
    }
  }

  case class OutputTopic[K,V](topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {
    def popOut( driver: () => Driver): Option[ProducerRecord[K,V]] = {
      Option(driver().readOutput(topicName, keyDeserializer, valueDeserializer))
    }
  }

  case class KVStore[K,V](storeName: String) {
    private def store(driver: Driver): KeyValueStore[K,V] = driver.getKeyValueStore(storeName)
    def get(driver: () => Driver)( key: K): Option[V] = Option(store(driver()).get(key))
    def put(driver: () => Driver)( key:K, value: V): Unit = store(driver()).put(key, value)
  }
}

package org.amitayh.invoices.streamprocessor

import java.util.Properties

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
    implicit val driver = before(props)
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



  case class InputTopic[K,V](val topicName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
    private val recordFactory = new ConsumerRecordFactory(keySerializer, valueSerializer)

    def pipeIn(key: K, value: V)(implicit driver: () => Driver): this.type = {
      driver().pipeInput(recordFactory.create(topicName, key, value))
      this
    }
  }

  case class OutputTopic[K,V](val topicName: String, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {
    def popOut(implicit driver: () => Driver): Option[ProducerRecord[K,V]] = {
      Option(driver().readOutput(topicName, keyDeserializer, valueDeserializer))
    }
  }

  case class KVStore[K,V](val storeName: String) {
    private def store(driver: Driver): KeyValueStore[K,V] = driver.getKeyValueStore(storeName)
    def get(key: K)(implicit driver: () => Driver): Option[V] = Option(store(driver()).get(key))
    def put( key:K, value: V)(implicit driver: () => Driver): Unit = store(driver()).put(key, value)
  }
}

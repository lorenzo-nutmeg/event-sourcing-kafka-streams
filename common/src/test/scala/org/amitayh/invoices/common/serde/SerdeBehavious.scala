package org.amitayh.invoices.common.serde

import org.apache.kafka.common.serialization.Serde
import org.scalatest.FlatSpec

trait SerdeBehavious { this: FlatSpec =>
  def roundtripSerDe[T](serde: Serde[T], t: T): Unit = {
    it should s"serialize the ${t.getClass.getSimpleName} and deserialize it back to the same value" in {
      val ser: Array[Byte] = serde.serializer.serialize("a-topic", t)
      assert(ser.size > 0, "Serialized content should not be empty")
      val des: T = serde.deserializer.deserialize("another-topic", ser)

      assert(des == t, "deserialized value must be equal to the initial value")
    }
  }
}

package br.com.emmanuelneri.schema;

import br.com.emmanuelneri.schema.orders.Order;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class KafkaAvroDeserializerTest {

    final private static KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    @Test
    public void shoulDeserializelWhenDeSerializeAValidRecordOnMappedTopic() {
        final Order record = Order.newBuilder()
                .setIdentifier("123")
                .setCustomer("Customer")
                .setValue(BigDecimal.valueOf(10.25))
                .build();

        final byte[] bytes = new KafkaAvroSerializer().serialize("orders", record);
        final Object object = deserializer.deserialize("orders", bytes);
        final Order order = (Order) object;
        Assertions.assertEquals("123", order.getIdentifier().toString());
        Assertions.assertEquals("Customer", order.getCustomer().toString());
        Assertions.assertEquals(BigDecimal.valueOf(10.25), order.getValue());
    }

    @Test
    public void shouldFailWhenDeSerializeInUnmappedTopic() {
        final br.com.emmanuelneri.schema.orders.Order record = Order.newBuilder()
                .setIdentifier("123")
                .setCustomer("Customer")
                .setValue(BigDecimal.TEN)
                .build();

        Assertions.assertThrows(SerializationException.class, () ->
                deserializer.deserialize("unknown-topic", record.toByteBuffer().array()));
    }

}
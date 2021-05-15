package br.com.emmanuelneri.schema;

import br.com.emmanuelneri.schema.orders.Order;
import org.apache.avro.AvroMissingFieldException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class KafkaAvroSerializerTest {

    final private static KafkaAvroSerializer serializer = new KafkaAvroSerializer();

    @Test
    public void shouldSerializeWhenSerializeAValidRecordOnMappedTopic() {
        final Order record = Order.newBuilder()
                .setIdentifier("123")
                .setCustomer("Customer")
                .setValue(BigDecimal.TEN)
                .build();

        final byte[] bytes = serializer.serialize("orders", record);
        Assertions.assertTrue(bytes.length > 0);
    }

    @Test
    public void shouldFailWhenSerializeInUnmappedTopic() {
        Assertions.assertThrows(SerializationException.class, () ->
                serializer.serialize("unknown-topic", Order.newBuilder()
                        .setIdentifier("123")
                        .setCustomer("Customer")
                        .setValue(BigDecimal.TEN)
                        .build()));
    }

    @Test
    public void shouldFailWhenSerializeRecordWithEmptyRequiredField() {
        Assertions.assertThrows(AvroMissingFieldException.class, () ->
                serializer.serialize("orders", Order.newBuilder()
                        .setCustomer("Customer")
                        .setValue(BigDecimal.TEN)
                        .build()));
    }

}
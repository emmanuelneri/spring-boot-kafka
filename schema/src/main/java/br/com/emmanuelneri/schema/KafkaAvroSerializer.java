package br.com.emmanuelneri.schema;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class KafkaAvroSerializer implements Serializer<Object> {

    private final Map<String, DatumWriter<Object>> datumWriterConfig = new ConcurrentHashMap<>();

    public KafkaAvroSerializer() {
        configureSchema("orders", br.com.emmanuelneri.schema.avro.Order.getClassSchema());
    }

    @Override
    public byte[] serialize(String topic, Object object) throws SerializationException {
        if (object == null) {
            return null;
        }

        if (!(object instanceof SpecificRecord)) {
            throw new SerializationException("record not implement SpecificRecord");
        }

        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

            final SpecificRecord record = (SpecificRecord) object;
            final DatumWriter<Object> datumWriter = this.datumWriterConfig.computeIfAbsent(topic, key -> {
                throw new SerializationException("Can't serialize object " + object
                        + " with schema " + record.getSchema().getFullName()
                        + " to topic " + topic);
            });

            datumWriter.write(record, encoder);
            encoder.flush();

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Can't serialize object " + object, e);
        }
    }

    private void configureSchema(final String topic, final Schema schema) {
        Stream.of(AvroSchemaConfig.values()).forEach(config ->
                datumWriterConfig.put(config.getTopic(), new SpecificDatumWriter<>(config.getSchema())));
    }

}
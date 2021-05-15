package br.com.emmanuelneri.schema;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class KafkaAvroDeserializer implements Deserializer<Object> {

    private final Map<String, SpecificDatumReader<Object>> datumReaderConfig = new ConcurrentHashMap<>();

    public KafkaAvroDeserializer() {
        configureSchema();
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final SpecificDatumReader<Object> datumReader = this.datumReaderConfig.computeIfAbsent(topic, key -> {
            throw new SerializationException("unmapped schema to topic " + topic);
        });

        try (final ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(bytes)) {
            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(byteArrayOutputStream, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Can't deserialize " + Arrays.toString(bytes) + "to topic " + topic, e);
        }
    }

    private void configureSchema() {
        Stream.of(AvroSchemaConfig.values()).forEach(config ->
                datumReaderConfig.put(config.getTopic(), new SpecificDatumReader<>(config.getSchema())));
    }
}

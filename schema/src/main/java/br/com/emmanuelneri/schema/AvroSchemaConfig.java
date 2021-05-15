package br.com.emmanuelneri.schema;

import org.apache.avro.Schema;

public enum AvroSchemaConfig {

    ORDER("orders", br.com.emmanuelneri.schema.avro.Order.getClassSchema());

    private final String topic;
    private final Schema schema;

    AvroSchemaConfig(String topic, Schema schema) {
        this.topic = topic;
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getTopic() {
        return topic;
    }
}

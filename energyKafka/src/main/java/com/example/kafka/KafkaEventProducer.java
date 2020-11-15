package com.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaEventProducer<K,V> implements AutoCloseable{

    private final KafkaProducer<K, V>  producer;
    private final String topicName;

    private KafkaEventProducer(String topicName) {
        producer = new KafkaProducer<K, V>(kafkaConfiguration());
        this.topicName = topicName;
    }


    private Properties kafkaConfiguration() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.99.100:8081");
        return properties;
    }

    public static <K,V> KafkaEventProducer<K,V> newInstance(String topicName) {
        return new KafkaEventProducer<K,V>(topicName);
    }


    public void produceRecord(V event) {

        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topicName, event);
        sendRecord(producerRecord);
    }

    private void sendRecord(ProducerRecord<K, V> record) {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null) {
                    System.out.println("New record sent to topic: " + topicName);
                    System.out.println(recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }
            }
        });
        producer.flush();
    }

    @Override
    public void close() throws Exception {
        producer.flush();
        producer.close();
    }
}

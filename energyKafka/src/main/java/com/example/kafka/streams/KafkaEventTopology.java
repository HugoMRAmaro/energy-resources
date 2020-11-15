package com.example.kafka.streams;

import com.example.BatteryEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.dropwizard.jdbi3.JdbiFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.jdbi.v3.core.Jdbi;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaEventTopology {

    private Properties kafkaStreamsConfiguration() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "charging-events");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.99.100:8081");
        return properties;
    }

    private Topology topology(Jdbi jdbi) {

        final Map<String, String> serdeConfig = Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://192.168.99.100:8081");

        final Serde<BatteryEvent> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values

        StreamsBuilder builder = new StreamsBuilder();

        // stream from kafka
        KStream<String, BatteryEvent> batteryEventStream = builder.stream("battery_event",
                Consumed.with(Serdes.String(), valueSpecificAvroSerde));

        // test this
        //batteryEventStream.print(Printed.toSysOut());

        batteryEventStream.process(()-> new KafkaDBProcessor(jdbi));

        /* kafka stream to db
        https://stackoverflow.com/questions/46524930/how-to-process-a-kafka-kstream-and-write-to-database-directly-instead-of-sending
        batteryEventStream.process();*/

        return builder.build();
    }

    public void start(Jdbi jdbi) {
        KafkaEventTopology kafkaEventTopology = new KafkaEventTopology();
        Topology topology = kafkaEventTopology.topology(jdbi);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, kafkaStreamsConfiguration());
        kafkaStreams.start();
    }


}

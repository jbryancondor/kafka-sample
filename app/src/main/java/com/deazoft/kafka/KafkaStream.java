package com.deazoft.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) {

        final var props = new Properties();;
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "groupIdJd-2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
        props.put("schema.registry.url","http://127.0.0.1:8081");
        props.put("auto.offset.reset", "earliest");
        final var builder = new StreamsBuilder();
        final var source = builder.stream("test", Consumed.with(Serdes.String(), Serdes.String()));
        source.mapValues(record->
                {
                    System.out.println(record.toString());
                    return record;
                })
                .selectKey((inputKey, outputEvent) -> "")
                .to(
                        (key, event, context) -> {
                            // We do not have access to the RecordMetadata of the outbound events.
                            // That's why we are logging only the outboundStarted.

                            return "test";
                        });

        final var streams =
                new KafkaStreams(builder.build(), props);

        streams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                System.out.println(newState.toString());
            }

        });
        streams.start();
        while (true){

        }
    }
}

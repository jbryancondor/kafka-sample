package com.deazoft.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("acks","1");
        properties.put("retries","3");

        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", KafkaJsonSchemaSerializer.class);

        properties.put("schema.registry.url","http://127.0.0.1:8081");
        properties.put("auto.register.schemas",false);
        //properties.put("value.subject.name.strategy",io.confluent.kafka.serializers.subject.TopicRecordNameStrategy.class);

        KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "test";

        var user = new User("bryan-3", "asdasd", "asdasdasd","asdasd");

        ProducerRecord<String, User> producerRecord = new ProducerRecord<>(
                topic,"test-1" ,user
        );

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println(recordMetadata);
                } else {
                    e.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}

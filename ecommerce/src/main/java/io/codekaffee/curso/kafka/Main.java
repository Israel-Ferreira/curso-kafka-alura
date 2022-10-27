package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ecommerce-producer");


        try(KafkaProducer<String,String> producer =  new KafkaProducer<>(props)) {
            String value =  "1223344,9999,99931";

            System.out.println("TESTE");

            ProducerRecord<String,String> producerRecord =  new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value,value);
            producer.send(producerRecord, (recordMetadata, e) -> {
                System.out.println(recordMetadata.topic());
            });

        }

    }
}
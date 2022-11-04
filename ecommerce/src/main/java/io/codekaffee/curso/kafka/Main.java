package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

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


            for (int i = 0; i < 100; i++) {
                var key = UUID.randomUUID().toString();

                ProducerRecord<String,String> producerRecord =  new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key ,value);
                Callback callback = (recordMetadata, e) -> System.out.println(recordMetadata.topic());



                producer.send(producerRecord, callback);


                String email = "Welcome! We are processing your order";

                ProducerRecord<String, String> emailRecord =  new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key  , email);
                producer.send(emailRecord);

            }


        }

    }
}
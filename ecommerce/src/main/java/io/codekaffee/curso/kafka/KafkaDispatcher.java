package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.UUID;

public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<String, T>(getProperties());
    }


    public void send(String topic, String key, T value){
        ProducerRecord<String,T> producerRecord =  new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key ,value);
        Callback callback = (recordMetadata, e) -> System.out.println(recordMetadata.topic());

        producer.send(producerRecord, callback);
    }


    private Properties getProperties(){
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());


        return props;
    }


    @Override
    public void close() {
        producer.close();
    }
}

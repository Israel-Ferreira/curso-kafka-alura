package io.codekaffee.curso.kafka;

import io.codekaffee.curso.kafka.models.ConsumerFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService implements Closeable {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction callback;



    public KafkaService(String consumerGroupName,String topic, ConsumerFunction callback) {
        this.consumer = new KafkaConsumer<>(getProperties(consumerGroupName));
        consumer.subscribe(Collections.singletonList(topic));

        this.callback = callback;
    }


    public void run(){
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                records.forEach(callback::consume);
            }
        }
    }


    private  Properties getProperties(String consumerGroupName){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());


        return props;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}

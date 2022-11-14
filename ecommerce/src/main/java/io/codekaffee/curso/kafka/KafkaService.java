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
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction<T> callback;



    public KafkaService(String consumerGroupName,String topic, ConsumerFunction<T> callback, Class<T> clazz) {
        this.consumer = new KafkaConsumer<>(getProperties(consumerGroupName, clazz));
        consumer.subscribe(Collections.singletonList(topic));

        this.callback = callback;
    }

   public KafkaService(String consumerGroupName, Pattern pattern, ConsumerFunction<T> callback, Class<T> clazz){
        this.consumer = new KafkaConsumer<String, T>(getProperties(consumerGroupName, clazz));
        consumer.subscribe(pattern);

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


    private  Properties getProperties(String consumerGroupName, Class<T> clazz){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(GsonDeserializer.TYPE_CONFIG, clazz.getName());


        return props;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}

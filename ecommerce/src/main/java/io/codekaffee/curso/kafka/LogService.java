package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var props = getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Pattern pattern = Pattern.compile("ECOMMERCE.*");

        consumer.subscribe(pattern);

        while (true){
            var records = consumer.poll(Duration.ofMillis(100));

            for(var record : records){
                System.out.println(record);
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.partition());
            }
        }
    }


    private static Properties getProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        return props;
    }
}

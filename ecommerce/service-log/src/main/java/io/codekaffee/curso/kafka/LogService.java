package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("ECOMMERCE.*");

        Map<String, Object> map = Map.of(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                GsonDeserializer.TYPE_CONFIG, String.class.getName()
        );

        try(var kafkaService = new KafkaService<String>(LogService.class.getName(), pattern, LogService::printRecord, String.class, map)){
            kafkaService.run();
        }

    }


    private static void printRecord(ConsumerRecord<String, String> record){
        System.out.println(record);
        System.out.println(record.key());
        System.out.println("TOPIC: " + record.topic());
        System.out.println(record.value());
        System.out.println(record.partition());
    }


}

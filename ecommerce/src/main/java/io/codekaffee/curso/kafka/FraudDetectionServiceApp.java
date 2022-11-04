package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectionServiceApp {
    public static void main(String[] args) throws InterruptedException {
        var props =  getProperties();
        var consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        while (true){
            var records = consumer.poll(Duration.ofMillis(100));

            if(!records.isEmpty()){
                for(var record : records){
                    System.out.println(record.key());
                    System.out.println(record.value());

                    System.out.println("Partição: " + record.partition());
                    System.out.println("Processando novo pedido:  " + record.value());
                    Thread.sleep(5000L);
                }
            }

        }
    }

    private static Properties getProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ECOMMERCE_FRAUD_SERVICE");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        return props;
    }
}

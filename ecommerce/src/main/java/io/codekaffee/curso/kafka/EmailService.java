package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        try(KafkaService kafkaService = new KafkaService(EmailService.class.getName(), "ECOMMERCE_SEND_EMAIL", EmailService::parseRecord);) {
            kafkaService.run();
        }

    }


    private static void parseRecord(ConsumerRecord<String, String> record){
        try {
            System.out.println(record.key());
            System.out.println(record.value());
            System.out.println("Processando novo pedido:  " + record.value());
            Thread.sleep(5000L);
        }catch (InterruptedException e){
            System.out.println(e.getLocalizedMessage());
        }
    }

}

package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectionServiceApp {
    public static void main(String[] args) {
        try(var kafkaService = new KafkaService<NewOrder>(FraudDetectionServiceApp.class.getName(), "ECOMMERCE_NEW_ORDER", FraudDetectionServiceApp::processAntiFraudDetection, NewOrder.class)) {
            kafkaService.run();
        }
    }

    private static void processAntiFraudDetection(ConsumerRecord<String, NewOrder> record){
        try {
            System.out.println(record.key());
            System.out.println(record.value());

            System.out.println("Partição: " + record.partition());
            System.out.println("Processando novo pedido:  " + record.value().getOrderId());
            Thread.sleep(5000L);
        }catch (InterruptedException e){
            System.out.println(e.getLocalizedMessage());
        }
    }


}

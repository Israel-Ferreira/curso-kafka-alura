package io.codekaffee.curso.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;

public class FraudDetectionServiceApp {

    private static final KafkaDispatcher<NewOrder> orderDispatcher = new KafkaDispatcher<>();

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

            var order = record.value();

            if(isFraud(order)){
                System.out.println("Order is a Fraud!!");
                orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
            }else{
                System.out.printf("Order %s is Approved!!%n", order);
                orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
            }

        }catch (InterruptedException e){
            System.out.println(e.getLocalizedMessage());
        }
    }

    private static boolean isFraud(NewOrder order) {
        return order.getValue().compareTo(new BigDecimal("4500")) >= 0;
    }


}

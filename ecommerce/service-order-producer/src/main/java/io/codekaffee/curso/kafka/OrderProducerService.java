package io.codekaffee.curso.kafka;

import io.codekaffee.curso.kafka.models.Email;
import io.codekaffee.curso.kafka.models.NewOrder;

import java.math.BigDecimal;
import java.util.UUID;

public class OrderProducerService {
    public static void main(String[] args) {
        try (var dispatcher = new KafkaDispatcher<NewOrder>();) {
            try(var emailDispatcher = new KafkaDispatcher<Email>()) {

                System.out.println("TESTE");

                for (int i = 0; i < 100; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    BigDecimal amount =  BigDecimal.valueOf(Math.random() * 50000 + 1);

                    NewOrder order = new NewOrder(userId, orderId, amount);
                    String key = String.format("%s-%s", userId, orderId);


                    dispatcher.send("ECOMMERCE_NEW_ORDER", key, order);


                    Email email = new Email("Welcome a board", "Welcome! We are processing your order");

                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
                }
            }

        }

    }
}
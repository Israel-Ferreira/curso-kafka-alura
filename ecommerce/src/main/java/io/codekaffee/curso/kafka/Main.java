package io.codekaffee.curso.kafka;

import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        try(var dispatcher = new KafkaDispatcher();) {

            String value =  "1223344,9999,99931";

            System.out.println("TESTE");


            for (int i = 0; i < 100; i++) {
                var key = UUID.randomUUID().toString();

                dispatcher.send("ECOMMERCE_NEW_ORDER", key ,value);

                String email = "Welcome! We are processing your order";

                dispatcher.send("ECOMMERCE_SEND_EMAIL", key  , email);

            }
        }



    }
}
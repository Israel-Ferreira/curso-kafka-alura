package io.codekaffee.curso.kafka.models;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigDecimal;

public class NewOrder {
    private  String userId;
    private  String orderId;
    private BigDecimal value;

    public NewOrder() {
    }

    public NewOrder(String userId, String orderId, BigDecimal value) {
        this.userId = userId;
        this.orderId = orderId;
        this.value = value;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().create();
        return gson.toJson(this);
    }
}

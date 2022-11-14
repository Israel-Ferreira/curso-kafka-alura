package io.codekaffee.curso.kafka.models;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Email {
    private String subject;
    private String body;

    public Email() {
    }

    public Email(String subject, String body) {
        this.subject = subject;
        this.body = body;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().create();
        return gson.toJson(this);
    }
}

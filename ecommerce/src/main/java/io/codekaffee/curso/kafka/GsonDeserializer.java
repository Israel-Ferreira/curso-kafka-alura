package io.codekaffee.curso.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new GsonBuilder().create();
    private Class<T> clazz;

    public static  String TYPE_CONFIG = "io.codekaffee.curso.kafka.type_config";


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(TYPE_CONFIG));

        try {
            this.clazz =  (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        System.out.println(clazz);
        return gson.fromJson(new String(bytes), clazz);
    }
}

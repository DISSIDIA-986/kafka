package com.dissidia986.kafka.examples.ch4;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public SimpleProducer() {
        properties.put("metadata.broker.list", "localhost:9093,localhost:9094");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer<>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new SimpleProducer();
        System.out.println("input topic:");
        Scanner terminalInput = new Scanner(System.in);
        String topic = terminalInput.nextLine();
        System.out.println("input msg:");
        String msg = terminalInput.nextLine();
        KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, msg);
        producer.send(data);
        producer.close();
    }
}
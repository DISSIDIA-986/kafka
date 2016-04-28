package com.dissidia986.kafka.examples.ch4;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.*;

public class MultiBrokerProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public MultiBrokerProducer() {
        properties.put("metadata.broker.list", "localhost:9092, localhost:9093");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("partitioner.class", "test.kafka.SimplePartitioner");
        properties.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<>(config);
    }

    public static void main(String[] args) {
        new MultiBrokerProducer();
        Random random = new Random();
        System.out.println("input topic:");
        Scanner terminalInput = new Scanner(System.in);
        String topic = terminalInput.nextLine();
        for (long i = 0; i < 10; i++) {
            Integer key = random.nextInt(255);
            String msg = "This message is for key - " + key;
            producer.send(new KeyedMessage<Integer, String>(topic, msg));
        }
        producer.close();
    }
}
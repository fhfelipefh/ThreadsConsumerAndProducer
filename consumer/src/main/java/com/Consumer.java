package com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumer is a class that consumes messages from a Kafka topic.
 * @author fhfelipefh
 */

public class Consumer implements Runnable {

    private final String TOPIC_TO_RECEIVE_MESSAGES = "messages";
    private static final String FEEDBACK_TOPIC = "feedback";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    private static final FeedbackProducer feedbackProducer = new FeedbackProducer(BOOTSTRAP_SERVERS, FEEDBACK_TOPIC);
    static int countZ = 0;

    public static void main(String[] args) {
        System.out.println("Consumer started");
        Thread thread = new Thread(new Consumer());
        feedbackProducer.sendFeedback("start");
        thread.start();
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(TOPIC_TO_RECEIVE_MESSAGES));

            while (true) {
                ConsumerRecords<String, String> cont = consumer.poll(Duration.ofMillis(100));
                if (cont.count() > 0) {
                    for (ConsumerRecord<String, String> entry : cont) {
                        processMessage(entry.value());
                        System.out.println(entry.value());
                    }
                }
            }
        }
    }

    private static void processMessage(String message) {
        if (message.matches("[aeiouAEIOU]")) {
            feedbackProducer.sendFeedback("4");
        }
        else {
            if (message.equals("Z")) countZ++;
            feedbackProducer.sendFeedback("2");
        }
        if (countZ > 20) {
            feedbackProducer.sendFeedback("Stop");
        }
    }

}
package com;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Producer is a class that is used to send messages to Kafka.
 * @author fhfelipefh
 */

public class Producer implements Runnable {

    private static final String TOPIC_TO_SENT_MESSAGES = "messages";
    private static final String FEEDBACK_TOPIC = "feedback";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:29092";
    private static FeedbackConsumer feedbackConsumer = new FeedbackConsumer(BOOTSTRAP_SERVERS, FEEDBACK_TOPIC);

    public static void main(String[] args) {
        System.out.println("Starting producer");
        feedbackConsumer.startProducerWithFeedback();
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        List<String> messages = getAlphabet();
        int i = 0;
        while (true) {
            if (messages.isEmpty()) {
                messages = getAlphabet();
                feedbackConsumer.sleepThread(10);
            }
            ProducerRecord<String, String> recordProducer = new ProducerRecord<>(TOPIC_TO_SENT_MESSAGES, messages.get(0));
            try {
                producer.send(recordProducer, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Message sent successfully");
                        } else {
                            System.out.println("Message sent failed");
                            System.out.println("Do you up docker-compose.yml file?");
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
            messages.remove(0);
        }
    }

    public static List<String> getAlphabet() {
        List<String> alphabet = new ArrayList<>();
        for (int i = 0; i < 26; i++) {
            alphabet.add(String.valueOf((char) (i + 65)));
        }
        return alphabet;
    }


}
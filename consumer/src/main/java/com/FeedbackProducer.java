package com;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class FeedbackProducer {

    private final String bootstrapServers;
    private final String feedbackTopicName;

    public FeedbackProducer(String bootstrapServers, String feedbackTopicName) {
        this.bootstrapServers = bootstrapServers;
        this.feedbackTopicName = feedbackTopicName;
    }

    public void sendFeedback(String feedback) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> recordProducer = new ProducerRecord<>(feedbackTopicName, feedback);

        try {
            producer.send(recordProducer, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("Feedback sent successfully");
                    } else {
                        System.out.println("feedback sent failed");
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


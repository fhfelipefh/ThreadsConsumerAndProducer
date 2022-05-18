package com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FeedbackConsumer {

    private final String bootstrapServers;
    private final String feedbackTopicName;
    private Thread producerThread;
    private Properties properties = new Properties();

    public FeedbackConsumer(String bootstrapServers, String feedbackTopicName) {
        this.feedbackTopicName = feedbackTopicName;
        this.producerThread = createNewThread();
        this.bootstrapServers = bootstrapServers;
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app");
    }

    public void startProducerWithFeedback() {
        startProducerThreadAndConsumeFeedback();
    }

    private void startProducerWithoutFeedback() {
        startThread();
    }

    private void startProducerThreadAndConsumeFeedback() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(feedbackTopicName));
            while (true) {
                ConsumerRecords<String, String> cont = consumer.poll(Duration.ofMillis(100));
                if (cont.count() > 0) {
                    switch (cont.iterator().next().value()) {
                        case "start":
                            System.out.println("Started");
                            producerThread = createNewThread();
                            startThread();
                            break;
                        case "Stop":
                            System.out.println("Stop");
                            producerThread.interrupt();
                            producerThread.stop();
                            break;
                        case "1":
                            System.out.println("Feedback received: 1");
                            sleepThread(1);
                            break;
                        case "2":
                            System.out.println("Feedback received: 2");
                            sleepThread(2);
                            break;
                        case "3":
                            System.out.println("Feedback received: 3");
                            sleepThread(3);
                            break;
                        case "4":
                            System.out.println("Feedback received: 4");
                            sleepThread(4);
                            break;
                        default:
                            System.out.println("Invalid Feedback received: " + cont.iterator().next().value());
                    }

                }

            }
        }
    }

    private Thread createNewThread() {
        return new Thread(new Producer());
    }

    private void startThread() {
        producerThread.start();
    }

    public void sleepThread(int i) {
        try {
            if (producerThread.isAlive() && !producerThread.isInterrupted()) {
                System.out.println("Thread is sleeping for " + i + " seconds");
                long seconds = (long) (i * 1000L);
                producerThread.sleep(seconds);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isFeedbackQueueEmpty() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(feedbackTopicName));
            ConsumerRecords<String, String> cont = consumer.poll(Duration.ofMillis(100));
            if (cont.count() > 0) {
                return false;
            }
        }
        return true;
    }

}

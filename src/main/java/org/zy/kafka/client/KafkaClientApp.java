package org.zy.kafka.client;

/**
 * Created by yuezhang on 18/1/28.
 */
public class KafkaClientApp {

    public static void main(String[] args)
    {
        KafkaProducer producerThread = new KafkaProducer(KafkaProperties.TOPIC);
        producerThread.start();
        KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.TOPIC);
        consumerThread.start();
    }

}

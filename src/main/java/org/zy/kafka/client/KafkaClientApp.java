package org.zy.kafka.client;

/**
 * Created by yuezhang on 18/1/28.
 */
public class KafkaClientApp {

    public static void main(String[] args) {

        KafkaProducer producer = new KafkaProducer(KafkaProperties.TOPIC);
        producer.start();

        KafkaConsumer consumer1 = new KafkaConsumer("t1",KafkaProperties.TOPIC,"group1");
        consumer1.start();

        KafkaConsumer consumer2 = new KafkaConsumer("t2",KafkaProperties.TOPIC,"group1");
        consumer2.start();


    }

}

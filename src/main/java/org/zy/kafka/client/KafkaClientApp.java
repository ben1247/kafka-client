package org.zy.kafka.client;

/**
 * Created by yuezhang on 18/1/28.
 */
public class KafkaClientApp {

    public static void main(String[] args) throws InterruptedException {

        KafkaProducerDemo producer = new KafkaProducerDemo(KafkaProperties.TOPIC);
        producer.start();

        KafkaConsumerDemo consumer1 = new KafkaConsumerDemo("t1",KafkaProperties.TOPIC,"group1");
        consumer1.start();

        KafkaConsumerDemo consumer2 = new KafkaConsumerDemo("t2",KafkaProperties.TOPIC,"group1");
        consumer2.start();

        Thread.sleep(20000);

        // 关闭
        producer.getProducer().close();
        consumer1.getConsumer().shutdown();
        consumer2.getConsumer().shutdown();

    }

}

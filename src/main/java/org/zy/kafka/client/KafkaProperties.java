package org.zy.kafka.client;

/**
 * Created by yuezhang on 18/1/27.
 */
public class KafkaProperties {

    final static String BROKER_LIST = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";

    final static String ZOOKEEPER_CONNECT = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    final static String TOPIC = "my-replicated-topic-3";

    final static int SEND_MSG_INTERVAL = 1000;

    final static int RECEIVE_MSG_INTERVAL = 2000;


}

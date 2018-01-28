package org.zy.kafka.client;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yuezhang on 18/1/27.
 */
public class KafkaConsumer extends Thread {

    private final ConsumerConnector consumer;

    private final String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
        this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    private static ConsumerConfig createConsumerConfig(){
        // Consumer实例所需的配置
        Properties properties = new Properties();

        // 指定了ZooKeeper的connect string，以hostname:port的形式，hostname和port就是ZooKeeper集群各个节点的hostname和port。
        // ZooKeeper集群中的某个节点可能会挂掉，所以可以指定多个节点的connect string。
        properties.put("zookeeper.connect",KafkaProperties.ZOOKEEPER_CONNECT);

        //group 代表一个消费组
        properties.put("group.id", KafkaProperties.GROUP_ID);

        // ZooKeeper的session的超时时间，如果在这段时间内没有收到ZK的心跳，则会被认为该Kafka server挂掉了。
        // 如果把这个值设置得过低可能被误认为挂掉，如果设置得过高，如果真的挂了，则需要很长时间才能被server得知。
        // 默认值：6000
        properties.put("zookeeper.session.timeout.ms", "40000");

        // 一个ZK follower能落后leader多久。
        // 默认值：2000
        properties.put("zookeeper.sync.time.ms", "2000");

        // consumer向ZooKeeper发送offset的时间间隔。
        // 默认值：60 * 1000
        properties.put("auto.commit.interval.ms", "60000");

        // 在Consumer在ZooKeeper中发现没有初始的offset时或者发现offset不在范围呢，该怎么做：
        // * smallest : 自动把offset设为最小的offset。
        // * largest : 自动把offset设为最大的offset。
        // * anything else: 抛出异常。
        // 默认值：largest
        properties.put("auto.offset.reset", "smallest");

        // 每一个获取某个topic的某个partition的请求，得到最大的字节数，每一个partition的要被读取的数据会加载入内存，
        // 所以这可以帮助控制consumer使用的内存。这个值的设置不能小于在server端设置的最大消息的字节数，
        // 否则producer可能会发送大于consumer可以获取的字节数限制的消息。
        // 默认值：1024 * 1024
        properties.put("fetch.message.max.bytes", "10485760");

        // 当一个新的consumer加入一个consumer group时，会有一个rebalance的操作，导致每一个consumer和partition的关系重新分配。
        // 如果这个重分配失败的话，会进行重试，此配置就代表最大的重试次数。
        properties.put("rebalance.max.retries", "10");

        // 在rebalance重试时的backoff时间。
        // 默认值：2000
        properties.put("rebalance.backoff.ms", "5000");

        //序列化类
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        return new ConsumerConfig(properties);
    }

    @Override
    public void run() {

        // 消费的Topic与消费线程数组成的Map
        Map<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put(topic,1);

        // 此处是StringDecoder，如果是其他的，可自行修改
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String,String>>> topicMessageStreams = consumer.createMessageStreams(topicThreadMap,keyDecoder,valueDecoder);

        KafkaStream<String, String> stream = topicMessageStreams.get(topic).get(0);

        ConsumerIterator<String,String> iterator = stream.iterator();

        while (iterator.hasNext()){
            System.out.println("receive：" + iterator.next().message());
            try {
                sleep(KafkaProperties.RECEIVE_MSG_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}

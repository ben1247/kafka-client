package org.zy.kafka.client;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by yuezhang on 18/1/27.
 */
public class KafkaProducerDemo extends Thread{

    private final Producer<Integer, String> producer;
    private final String topic;


    public KafkaProducerDemo(String topic){
        this.topic = topic;

        final Properties props = new Properties();
        props.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //request.required.acks可以设定的值分别是0，1，-1
        //0代表客户端不需要服务端将消息保存成功的反馈
        //1代表客户端需要服务端将消息在Partition的Leader上保存成功的反馈
        //-1代表客户端需要服务端将消息在Partition的所有副本上保存成功的反馈
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (messageNo <= 20){
            String messageStr = "Message_" + messageNo;
            System.out.println("Send: " + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
            try {
                sleep(KafkaProperties.SEND_MSG_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

package com.yinchen.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread{
    private String topic;

    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector() {

        Properties properties = new Properties();

        properties.put("zookeeper.connect", KafkaProperties.ZK);
        //we also need a group-id,
        properties.put("group.id", KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        //消费的时候使用的是zookeeper
    }

    @Override
    public void run(){
        ConsumerConnector consumer = createConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
//        topicCountMap.put(topic2, 1);
//        topicCountMap.put(topic3, 1);

        //key - String: topic
        //value - List<KafkaStream<byte[], byte[]>> : 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0);//我们每次接收到的数据

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while(iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("receive: " + message);

        }
    }
}

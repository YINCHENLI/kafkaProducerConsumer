package com.yinchen.spark.kafka;

/**
 * kafka Java API testing 测试
 */
public class KafkaClientApp {
    public static void main(String[] args) {

        new KafkaProducer(KafkaProperties.TOPIC).start();
    }
}

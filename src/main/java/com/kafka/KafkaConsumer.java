package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.131:6667");
        //指定促消费者组
        props.put("group.id", "group1");
        //指定key/value的反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");

        //创建一个kafka消费者
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
        //消费者订阅指定topic
        consumer.subscribe(Arrays.asList("jlpowerTopic"));
        TopicPartition tp =new TopicPartition("jlpowerTopic",0);
        int index =0;
        while(true) {
            //消费者通过poll主动拉取的方式获得topic中的records
            ConsumerRecords<String, String> records = consumer.poll(100);
            //指定从哪个offset开始读取
            consumer.seek(tp, 666642);
            for (ConsumerRecord<String, String> record : records) {
                index++;
                System.out.printf("offset = %d, key = %s, value = %s, topic = %s, partition = %d",
                        record.offset(), record.key(), record.value(), record.topic(), record.partition());
                System.out.println();
                if (index==20)break;
            }
            if (index==20)break;
        }
    }
}

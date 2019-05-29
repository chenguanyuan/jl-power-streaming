package com.kafka;

import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
//创建kafka topic
// bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic jlpowerTopic

//执行kafka发送程序
//args:本地文件路径 kafkatopic 发送时间间隔，单位为秒
//java -cp /home/spark/cgy/sparkstreaming/jl-power-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar /home/spark/cgy/sparkstreaming/jlpowertestdata jlpowerTopic 1
public class kafkaProducer {
    public static void main(String[] args) throws IOException {
        //本地文件路径
        String path = args[0];
        //指定发送数据的topic
        String topic = args[1];
        //发送时间间隔，单位为秒
        int duration = Integer.valueOf(args[2]);

        Properties props = new Properties();
        //kafka集群的连接地址
        Properties properties = new Properties();
        InputStream in = kafkaProducer.class.getClassLoader().getResourceAsStream("DB.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            in.close();
        }
        String dbName = properties.getProperty("dbname");
        if("oracle".equals(dbName)){props.put("bootstrap.servers", "192.168.2.131:6667");}
        else{props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");}
        //key和value的序列化类，这里是String类型
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定消息的分区器，默认就是DefaultPartitioner：
        //当key为空时，顺序轮询发送到topic的每个partition；
        //当key不为空时，按照key的hash值%分区数，发送给每个topic的partition;这点上和spark或mr的HashPartitioner是一样的
        props.put("partitioner.class","org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        //设置producer消息缓冲大小，单位为byte，默认为16384bytes，即16KB
        //当缓冲数据大小直到16384bytes才会发送给kafka的brokers
        props.put("batch.size", "16384");
        //当batch内有数据，且sender等待时间超过linger.ms，则同样会把batch内的数据发送给kafka的broker。默认为0，即有消息就立即发送
        //因此，producer会给brokers发送消息的情况有两种：
        //1、当数据量超过batch.size；2、当batch内有数据且等待发送时间超过linger.ms
        props.put("linger.ms","5");

        //创建kafka的producer，其中Object为key类型，String为value类型
        KafkaProducer producer = new KafkaProducer<Object, String>(props);

        //读取文件
        //InputStream in = kafkaProducer.class.getClassLoader().getResourceAsStream("text.txt");
        File file = new File(path);
        File[] tempList = file.listFiles();
        Arrays.sort(tempList);
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                System.out.println(tempList[i].getName());
                inputStream = new FileInputStream(tempList[i]);
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String data = null;
                while((data = bufferedReader.readLine()) != null){
                    try {
                        //每duration秒发送一次
                        TimeUnit.MILLISECONDS.sleep(duration);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //向kafka集群发送数据，ProducerRecord类为每条数据的封装类
                    producer.send(new ProducerRecord<Object, String>(topic, data));
                }
            }
        }
        inputStream.close();
        bufferedReader.close();
    }
}

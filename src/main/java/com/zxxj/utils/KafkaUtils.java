package com.zxxj.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaUtils {
    private static KafkaProducer<String, String> kp;

    public static KafkaProducer<String, String> getProducer() {
        String kafkaBroker = PropertiesUtil.getString("kafkaBrokers");
        int lingerMs = PropertiesUtil.getInt("linger.ms");
        int batchSize = PropertiesUtil.getInt("batch.size");
        String acks = PropertiesUtil.getString("acks");
        String retries = PropertiesUtil.getString("retries");
        if (kp == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaBroker);
            props.put("acks", acks);
            props.put("retries", retries);
            props.put("linger.ms", lingerMs);
            props.put("batch.size", batchSize);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kp = new KafkaProducer<String, String>(props);
        }
        return kp;
    }
    public static void sendMsg(String topic, String sendKey, String data){
        Producer<String, String> producer = getProducer();
        //原来的现在测试是否兼容
        producer.send(new ProducerRecord<String, String>(topic,sendKey,data));
    }
    public static void main(String[] args){
        for(int i=0;i<10;i++){
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendMsg("testStream","testCanalKet","testCanalVal");

        }

    }

}

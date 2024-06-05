package org.example;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class Hello01Producer extends Thread{

    private Producer<String, String> producer;

    public Hello01Producer(String pname){
        super.setName(pname);
        Properties properties = new Properties();

        // kafka 地址
        properties.put("bootstrap.servers", "localhost:9092");
        // 配置序列化機制
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 請求應答模式 : 0、1、all  ( at least once / at most once / exactly once )
        properties.put("acks", "1");

        // 批次輸出 ( 如果儲存的資料超過這個大小就發送, 避免多次發送小資料消耗資源 )
        properties.put("batch.size", "16384");

        // 但如果一直達不到 batch.size 而使訊息卡住, 可以用這個屬性去控制, 時間到就推送當前累積的訊息.
        properties.put("linger.ms", 5000); // 设置 linger.ms 为 100 毫秒

        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run(){

        int count = 0;

        System.out.println("Hello01Producer.run -- 開始發送數據");

        while(count < 1000){
            String key = String.valueOf(++count);
            String value = Thread.currentThread().getName() + "--" + count;

            // 指定 topic name 和 key / value
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);
            // 生產者發送消息
            producer.send(record);

            System.out.println("run --" + key + "--" + value);

            try{
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
//                throw new RuntimeException(e);
            }

        }
    }

}

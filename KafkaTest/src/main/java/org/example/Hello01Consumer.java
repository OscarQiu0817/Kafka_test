package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Hello01Consumer extends Thread{

    private Consumer<String, String>  consumer;

    public Hello01Consumer(String cname){

        super.setName(cname);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // 消費者所屬的 group id  <= 這個步驟就是在設定, 所以不需要先建立一個 group 之後, 再讓這個消費者加入到 group
        props.put("group.id", "test-group");

        // 反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 偏移量設定策略：如果沒有初始偏移量，則從最前面的資料開始處理
        props.put("auto.offset.reset", "earliest"); // 也可以是 "latest" 或 "none"

        // session 超時
        props.put("session.timeout.ms", "30000");
        // 心跳檢測間隔 ( 必須小於超時 )
        props.put("heartbeat.interval.ms", "3000");

        // 自動提交，默認為 true
        props.put("enable.auto.commit", "true");
        // 自動提交偏移量的時間間隔 ( 須注意處理數據的時間, 如果數據處理中, 卻被提交了, 就會發生無法再次消費此數據的情況 => 數據丟失 )
        props.put("auto.commit.interval.ms", "1000");

        consumer = new KafkaConsumer<>(props);

        // 訂閱這個 topic
        consumer.subscribe(Collections.singletonList("test-topic"));

    }

    @Override
    public void run(){

        System.out.println("Hello01Consumer.run -- 開始拉取數據");

        // 每 0.1 秒輪詢一次 topic內資料並印出
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record ->
                    System.out.printf(Thread.currentThread().getName() + " consumed %s : -- with key %s and value %s%n",
                            record.offset(), record.key(), record.value()));
        }

    }



}

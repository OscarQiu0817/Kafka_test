package org.example;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        Hello01Producer producer = new Hello01Producer("kafka-producer1");
        Hello01Consumer consumer = new Hello01Consumer("kafka-consumer1");

        // 每 100ms 產生數據, 每 500ms 放到 topic
        producer.start();
        // 每 100 秒輪詢
        consumer.start();

        /*
        console輸出如下 :

            run --876--kafka-producer1--876
            run --877--kafka-producer1--877
            run --878--kafka-producer1--878
            run --879--kafka-producer1--879
            run --880--kafka-producer1--880
            kafka-consumer1consumed 1275 : -- with key 876 and value kafka-producer1--876
            kafka-consumer1consumed 1276 : -- with key 877 and value kafka-producer1--877
            kafka-consumer1consumed 1277 : -- with key 878 and value kafka-producer1--878
            kafka-consumer1consumed 1278 : -- with key 879 and value kafka-producer1--879
            kafka-consumer1consumed 1279 : -- with key 880 and value kafka-producer1--880
         */

    }
}
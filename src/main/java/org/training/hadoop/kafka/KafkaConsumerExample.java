package org.training.hadoop.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class KafkaConsumerExample {
    //config
    public static Properties getConfig() {
        Properties props = new Properties();
        //初始化Broker列表
        props.put("bootstrap.servers", "localhost:9092");
        //Consumer所在的group
        props.put("group.id", "testGroup");
        //是否⾃动提交offset
        props.put("enable.auto.commit", "true");
        //Key和value反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    public void consumeMessage() {
        // launch 3 threads to consume
        int numConsumers = 3;
        final String topic = "test1";
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        final List<KafkaConsumerRunner> consumers = new ArrayList<KafkaConsumerRunner>();
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumerRunner consumer = new KafkaConsumerRunner(topic);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (KafkaConsumerRunner consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // Thread to consume kafka data
    public static class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        //KafkaConsumer 与Kafka broker通信的类，常⽤⽅法是subscribe和poll
        private final KafkaConsumer<String, String> consumer;
        private final String topic;

        public KafkaConsumerRunner(String topic) {
            Properties props = getConfig();
            consumer = new KafkaConsumer<String, String>(props);
            this.topic = topic;
        }

        public void handleRecord(ConsumerRecord record) {
            System.out.println("name: " + Thread.currentThread().getName() + " ; topic: " + record.topic() + " ; offset" + record.offset() + " ; key: " + record.key() + " ; value: " + record.value());
        }

        public void run() {
            try {
                // subscribe
                consumer.subscribe(Arrays.asList(topic));
                while (!closed.get()) {
                    //read data 读取数据
                    //ConsumerRecords ⼀组返回结果，保存每个partition对应的结果集
                    ConsumerRecords<String, String> records = consumer.poll(10000);
                    // Handle new records
                    for (ConsumerRecord<String, String> record : records) {
                        //ConsumerRecord ⼀个返回结果，包含topic，offset，key/value，timestamp等信息
                        handleRecord(record);
                    }
                }
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        KafkaConsumerExample example = new KafkaConsumerExample();
        example.consumeMessage();
    }
}

package org.training.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class KakfaProducerExample {
    public void produceMessage() {
        Properties props = getConfig();
        //KafkaProducer 与Kafka broker通信的类
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 1000; i++) {

            // ProducerRecord 封装了 topic,	key,value, partition和timestamp
            //同步发送
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));

            //异步发送
//            ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("thetopic", "key", "value");  producer.send(myRecord,
//                    new Callback() {
//                        public void onCompletion(RecordMetadata metadata, Exception e) {
//                            if(e != null)
//                                e.printStackTrace();
//                            System.out.println("The offset of the record we just sent is: "
//                                    + metadata.offset());
//                        }
//                    });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    // config
    public Properties getConfig() {
        Properties props = new Properties();
        ////初始化Broker列表
        props.put("bootstrap.servers", "localhost:9092");
        //可靠性级别：可选值：0，1(default)，all
        props.put("acks", "all");
        props.put("retries", 0);
        //每个batch数据量
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        //Producer数据缓存⼤⼩（等到满后会发给broker）
        props.put("buffer.memory", 33554432);
        //Key和value序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void main(String[] args) {
        KakfaProducerExample example = new KakfaProducerExample();
        example.produceMessage();
    }
}

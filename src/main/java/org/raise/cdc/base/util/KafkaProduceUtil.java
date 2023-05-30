package org.raise.cdc.base.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Description: kafka读写工具
 * @Author: WangYouzheng
 * @Date: 2021/12/28 10:11
 * @Version: V1.0
 */
public class KafkaProduceUtil {
    //public static final String KAFKA_SERVER = "10.123.175.195:9092,10.123.175.196:9092,10.123.175.197:9092";
    //public static final String KAFKA_SERVER = "192.168.3.195:9092,192.168.3.96:9092,192.168.3.97:9092";
    public static final String KAFKA_SERVER = PropertiesUtil.getPropsStr("kafka.hostname");
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    /**
     * 获取指定主题的生产者。。  通过制定序列化方案的方式
     *
     * @param topic
     * @param <T>
     * @return
     */
    public static <K, T> KafkaProducer<K, T> getKafkaProductBySchema(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        //设置生产数据的超时时间
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 12 * 60 * 1000 + "");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
}

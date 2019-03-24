package kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerInstance {

    /**
     * 自动提交offset
     * @param prop
     */
    public static KafkaConsumer<String, String> autoSubmitOffset(Properties prop) {
        Properties props = new Properties();
        //必须要加，如果要读旧数据.默认是largest
        props.put("auto.offset.reset", "smallest");
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", prop.get("bootstrap.server"));
        /* 制定consumer group */
        props.put("group.id", "test1");
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        return consumer;
    }

    /**
     * 手动提交offset
     * @param prop
     */
    public static KafkaConsumer<String, String> manualSubmitOffset(Properties prop) {
        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", prop.get("bootstrap.server"));
        /* 制定consumer group */
        props.put("group.id", "test1");
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "false");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        return consumer;
    }

}

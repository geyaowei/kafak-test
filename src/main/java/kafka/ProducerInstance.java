package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class ProducerInstance {

    public Producer<String, String> producer;

    public ProducerInstance(Properties props) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", props.get("bootstrap.server"));
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    public void close(){
        this.producer.close();
    }


}

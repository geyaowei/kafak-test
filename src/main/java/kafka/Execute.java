package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.PropertiesUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Execute {

    public static void producer(){
        Properties propertiesUtil =PropertiesUtil.readProperties();
        ProducerInstance instance = new ProducerInstance(propertiesUtil);
        for (int i = 0; i < 10; i++) {
            String msg = "This is Message " + i;
            instance.producer.send(new ProducerRecord<String, String>("test", msg));
            System.out.println("Sent:" + msg);
            try{
                Thread.sleep(5000);
            }catch (Exception e){
                System.out.println("error: "+e);
            }

        }
    }

    public static void consumer(){
        Properties propertiesUtil =PropertiesUtil.readProperties();
        KafkaConsumer<String, String> consumer =ConsumerInstance.autoSubmitOffset(propertiesUtil);
        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records =consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
                count++;
            }
            System.out.println("sum: "+count);
        }
    }

    public static void consumerManual(){
        Properties propertiesUtil =PropertiesUtil.readProperties();
        KafkaConsumer<String, String> consumer =ConsumerInstance.manualSubmitOffset(propertiesUtil);
        final int minBatchSize = 5;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records =consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            /* 数据达到批量要求，同步确认offset */
            if (buffer.size() >= minBatchSize) {
                System.out.println("data save size: "+buffer.size());
                //提交offset
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    public static void main(String[] args){
        Execute.consumerManual();
        //Execute.producer();
    }
}

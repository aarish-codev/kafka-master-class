package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String boostrapServers = "localhost:9092";
        String groupId = "kafka-java-app";
        String topic = "demo-java";

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // subscribe to topic
        kafkaConsumer.subscribe(Arrays.asList(topic));
        //poll for new data
        while (true) {

            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> r : records) {
                log.info("\nKey: {}\nValue: {}", r.key(), r.value());
                log.info("\nPartition: {}\nOffset: {}", r.partition(), r.offset());
            }

        }
    }
}
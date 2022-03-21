package producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String topicName = "demo-java";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        try (AdminClient kafkaAdminClient = KafkaAdminClient.create(properties)) {
            // create Topic
            NewTopic topic = new NewTopic(topicName, 3, (short) 1);
            CreateTopicsResult result = kafkaAdminClient.createTopics(
                    Arrays.asList(topic)
            );
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        // create Producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, "hello");
        //send data
        kafkaProducer.send(producerRecord);
        //send data with Callback
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null)
                log.info("Recieved data\n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Key: " + producerRecord.key() + "\n" +
                        "Timestamp: " + Instant.ofEpochMilli(metadata.timestamp()));
            else
                log.error("Error while producing record");
        });
        //flush data - synchronous
        kafkaProducer.flush();
        // close Producer
        kafkaProducer.close();

    }
}
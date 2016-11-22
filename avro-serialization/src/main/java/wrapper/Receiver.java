package wrapper;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Receiver<K, V> {

    private static final Logger LOG = Logger.getLogger(Receiver.class);

    private final Consumer<K, V> consumer;

    private final String topic;

    public Receiver(String topic, boolean specificAvroReader) {

        this.topic = topic;

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_id");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("specific.avro.reader", String.valueOf(specificAvroReader));

        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        LOG.info("Created Kafka Consumer for topic " + topic);

    }

    public List<V> receive() {

        List<V> buffer = new ArrayList<>();

        ConsumerRecords<K, V> records;
        do {
            records = consumer.poll(100);
            if (records.count() > 0) {
                for (ConsumerRecord<K, V> record : records) {
                    buffer.add(record.value());
                }
                consumer.commitSync();
                consumer.close();
                LOG.info("Consumer closed successfully");
            }
        } while (records.count() == 0);
        return buffer;
    }
}

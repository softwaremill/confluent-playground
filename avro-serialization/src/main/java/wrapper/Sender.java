package wrapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

public class Sender<K, V> {

    private static final Logger LOG = Logger.getLogger(Sender.class);


    private final Producer<K, V> producer;
    private final String topic;

    public Sender(String topic) {

        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");

        this.producer = new KafkaProducer<>(properties);
        LOG.info("Created Kafka Producer for topic " + topic);

    }

    public Future<RecordMetadata> send(V message) {

        ProducerRecord<K, V> record = new ProducerRecord<>(topic, message);
        try {
            return producer.send(record);
        } catch(SerializationException se) {
            LOG.error("Couldn't send message:", se);
            throw se;
        }

    }

    public void close() {
        LOG.info("Closing Kafka Producer");
        producer.close();
    }

}

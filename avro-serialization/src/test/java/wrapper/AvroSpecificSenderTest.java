package wrapper;

import keyar.domain.Metric;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class AvroSpecificSenderTest {

    private static final Logger LOG = Logger.getLogger(AvroSpecificSenderTest.class);

    @Test
    public void testAvroSerialization() throws Exception {

        // given

        // this is an autogenrated class, need to run maven clean package to get it
        // $ mvn clean package -DskipTests
        Metric m1 = new Metric();
        m1.setIp("IP");
        m1.setName("ABC");
        m1.setValue(123f);

        Metric m2 = new Metric("IP", "ABC", 234f);

        Metric m3 = Metric.newBuilder().setIp("IP").setName("ABC").setValue(345f).build();

        String topic = UUID.randomUUID().toString();
        Sender<Object, Metric> sender = new Sender<>(topic);

        // when
        Future result1 = sender.send(m1);
        Future result2 = sender.send(m2);
        Future result3 = sender.send(m3);

        // then
        RecordMetadata record = (RecordMetadata) result1.get();
        Assert.assertEquals(topic, record.topic());
        record = (RecordMetadata) result2.get();
        Assert.assertEquals(topic, record.topic());
        record = (RecordMetadata) result3.get();
        Assert.assertEquals(topic, record.topic());

        Receiver<Object, GenericRecord> receiver = new Receiver<>(topic);
        List<GenericRecord> results = receiver.receive();
        results.forEach((result) -> {
            Metric metric = (Metric) SpecificData.get().deepCopy(Metric.SCHEMA$, result);
            LOG.info(metric);
        });

        Assert.assertEquals(3, results.size());

        Assert.assertEquals("IP", results.get(0).get("ip").toString());
        Assert.assertEquals("ABC", results.get(0).get("name").toString());
        Assert.assertEquals(123f, results.get(0).get("value"));

    }

}

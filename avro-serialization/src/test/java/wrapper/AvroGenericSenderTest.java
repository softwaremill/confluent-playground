package wrapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class AvroGenericSenderTest {

    private static final Logger LOG = Logger.getLogger(AvroGenericSenderTest.class);

    private static final String SIMPLE_SCHEMA =
            "{\"namespace\": \"keyar.domain\", \"type\": \"record\", " +
                    "\"name\": \"generic_avro\"," +
                    "\"fields\": [" +
                    "{\"name\": \"name\", \"type\": \"string\"}," +
                    "{\"name\": \"value\", \"type\": \"float\"}" +
                    "]}";

    private static final String INCOMPATIBLE_SCHEMA = "{\"namespace\": \"keyar.domain\", \"type\": \"record\", " +
            "\"name\": \"generic_avro\"," +
            "\"fields\": [" +
            "{\"name\": \"ip\", \"type\": \"string\"}," + // new incompatible field
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"value\", \"type\": \"float\"}" +
            "]" +
            "}";

    private static final String COMPATIBLE_SCHEMA = "{\"namespace\": \"keyar.domain\", \"type\": \"record\", " +
            "\"name\": \"generic_avro\"," +
            "\"fields\": [" +
            "{\"name\": \"ip\", \"type\": \"string\", \"default\": \"DEFAULT\"}," + // new compatible field
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"value\", \"type\": \"float\"}" +
            "]" +
            "}";

    @Test
    public void testAvroSerialization() throws Exception {

        // given
        String topic = UUID.randomUUID().toString();
        Sender<Object, GenericRecord> sender = new Sender<>(topic);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SIMPLE_SCHEMA);

        // when
        GenericRecord metric = new GenericData.Record(schema);
        metric.put("name", "ABC");
        metric.put("value", 123f);

        Future result = sender.send(metric);

        // then
        RecordMetadata record = (RecordMetadata) result.get();
        Assert.assertEquals(topic, record.topic());
        sender.close();

        // receive
        Receiver<Object, GenericRecord> receiver = new Receiver<>(topic, false);
        List<GenericRecord> results = receiver.receive();
        results.forEach(LOG::info);

        Assert.assertEquals(1, results.size());

        Assert.assertEquals("ABC", results.get(0).get("name").toString());
        Assert.assertEquals(123f, results.get(0).get("value"));

    }

    /**
     * Should throw SerializationException, since the schema is not backward compatible
     * Adding a new field to the schema does not allow the reader to read messages persisted with older schemas,
     * if no default value is given
     */
    @Test
    public void testAvroBackwardIncompatibleSerialization() throws Exception {


        // given
        String topic = UUID.randomUUID().toString();
        Sender<Object, GenericRecord> sender = new Sender<>(topic);

        Schema.Parser parser1 = new Schema.Parser();
        Schema.Parser parser2 = new Schema.Parser();
        Schema simpleSchema = parser1.parse(SIMPLE_SCHEMA);
        Schema incompatibleSchema = parser2.parse(INCOMPATIBLE_SCHEMA);

        // when
        GenericRecord metric1 = new GenericData.Record(simpleSchema);
        metric1.put("name", "ABC");
        metric1.put("value", 123f);

        GenericRecord metric2 = new GenericData.Record(incompatibleSchema);
        metric2.put("ip", "IP");
        metric2.put("name", "ABC");
        metric2.put("value", 123f);

        sender.send(metric1);
        try {
            sender.send(metric2);
            Assert.fail("Should not send due to serialization exception");
        } catch (SerializationException se) {
            // expected
        }
    }

    /**
     * Should not throw SerializationException, since the schema is backward compatible
     * Adding a new field to the schema does allow the reader to read messages persisted with older schemas,
     * if a default value is given
     */
    @Test
    public void testAvroBackwardCompatibleSerialization() throws Exception {


        // given
        String topic = UUID.randomUUID().toString();
        Sender<Object, GenericRecord> sender = new Sender<>(topic);

        Schema.Parser parser1 = new Schema.Parser();
        Schema.Parser parser2 = new Schema.Parser();
        Schema simpleSchema = parser1.parse(SIMPLE_SCHEMA);
        Schema compatibleSchema = parser2.parse(COMPATIBLE_SCHEMA);

        // when
        GenericRecord metric1 = new GenericData.Record(simpleSchema);
        metric1.put("name", "ABC");
        metric1.put("value", 123f);

        GenericRecord metric2 = new GenericData.Record(compatibleSchema);
        metric2.put("ip", "IP");
        metric2.put("name", "ABC");
        metric2.put("value", 123f);

        Future result1 = sender.send(metric1);
        Future result2 = sender.send(metric2);

        // then
        RecordMetadata record = (RecordMetadata) result1.get();
        Assert.assertEquals(topic, record.topic());
        record = (RecordMetadata) result2.get();
        Assert.assertEquals(topic, record.topic());

        // receive
        Receiver<Object, GenericRecord> receiver = new Receiver<>(topic, false);
        List<GenericRecord> results = receiver.receive();
        results.forEach(LOG::info);

        Assert.assertEquals(2, results.size());

        Assert.assertEquals("ABC", results.get(0).get("name").toString());
        Assert.assertEquals(123f, results.get(0).get("value"));

        Assert.assertEquals("IP", results.get(1).get("ip").toString());
        Assert.assertEquals("ABC", results.get(1).get("name").toString());
        Assert.assertEquals(123f, results.get(1).get("value"));
    }


    @Test
    public void testAvroSimpleSerialization() throws Exception {

        // given
        String topic = UUID.randomUUID().toString();
        Sender<Object, String> sender = new Sender<>(topic);

        // when
        Future result = sender.send("Test Data");

        // then
        RecordMetadata record = (RecordMetadata) result.get();
        Assert.assertEquals(topic, record.topic());

        // receive
        Receiver<Object, String> receiver = new Receiver<>(topic, false);
        List<String> results = receiver.receive();
        results.forEach(LOG::info);

        Assert.assertEquals(1, results.size());

        Assert.assertEquals("Test Data", results.get(0).toString());

    }
}

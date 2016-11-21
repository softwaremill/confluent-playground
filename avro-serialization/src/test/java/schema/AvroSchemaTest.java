package schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class AvroSchemaTest {

    @Test
    public void shouldCreateStandardSchema() {
        //given

        //when
        Schema schema = SchemaBuilder
                .record("HandshakeRequest").namespace("com.softwaremill.schema")
                .fields()
                .name("clientHash").type().fixed("MD5").size(16).noDefault()
                .name("clientProtocol").type().nullable().stringType().noDefault()
                .name("serverHash").type("MD5").noDefault()
                .name("meta").type().nullable().map().values().bytesType().noDefault()
                .endRecord();

        //then
        System.out.println(schema.toString());
    }

    @Test
    public void shouldCreateSchemaWithUnion() {
        //given
        Schema smsSchema = SchemaBuilder
                .record("Sms").namespace("com.softwaremill.schema")
                .fields()
                .name("phoneNumber").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();

        Schema emailSchema = SchemaBuilder
                .record("Email").namespace("com.softwaremill.schema")
                .fields()
                .name("addressTo").type().stringType().noDefault()
                .name("title").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();

        Schema pnSchema = SchemaBuilder
                .record("PushNotification").namespace("com.softwaremill.schema")
                .fields()
                .name("arn").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();

        //when
        Schema schema = SchemaBuilder
                .record("MessageToSend").namespace("com.softwaremill.schema")
                .fields()
                .name("text").type().stringType().noDefault()
                .name("correlationId").type().stringType().noDefault()
                .name("payload").type().unionOf()
                    .record("Sms").namespace("com.softwaremill.schema")
                    .fields()
                    .name("phoneNumber").type().stringType().noDefault()
                    .name("text").type().stringType().noDefault()
                    .endRecord().and()
                    .record("Email").namespace("com.softwaremill.schema")
                    .fields()
                    .name("addressTo").type().stringType().noDefault()
                    .name("title").type().stringType().noDefault()
                    .name("text").type().stringType().noDefault()
                    .endRecord()
                    .and()
                    .record("PushNotification").namespace("com.softwaremill.schema")
                    .fields()
                    .name("arn").type().stringType().noDefault()
                    .name("text").type().stringType().noDefault()
                    .endRecord()
                    .endUnion()
                    .noDefault()
                .endRecord();

        //then
        System.out.println(schema.toString());

        //when
        Schema schemaV2 = SchemaBuilder
                .record("MessageToSendV2").namespace("com.softwaremill.schema")
                .fields()
                .name("text").type().stringType().noDefault()
                .name("correlationId").type().stringType().noDefault()
                .name("payload").type().unionOf()
                .record("Sms").namespace("com.softwaremill.schema")
                .fields()
                .name("phoneNumber").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord().and()
                .record("Email").namespace("com.softwaremill.schema")
                .fields()
                .name("addressFrom").type().stringType().stringDefault("andrzej@test.pl")
                .name("addressTo").type().stringType().noDefault()
                .name("title").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord()
                .and()
                .record("PushNotification").namespace("com.softwaremill.schema")
                .fields()
                .name("arn").type().stringType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord()
                .endUnion()
                .noDefault()
                .endRecord();

        //then
        System.out.println(schemaV2.toString());
    }
}

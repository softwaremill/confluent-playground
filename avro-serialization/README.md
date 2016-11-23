# Introduction
This module is about using the Schema Registry and the corresponding Avro serializer and deserializer to exchange messages through Kafka.

It is based on Confluent's examples which can be found here:
https://github.com/confluentinc/examples

Also worth to watch an introduction to Confluent's Schema Registry to get a basic idea what it is about:
https://vimeo.com/167028700

The corresponding blog post is available at:
https://softwaremill.com/dog-ate-my-schema

# Prerequisites
The Avro "specific" example relies on a class (Metric.class), which is auto-generated based on *src/main/resources/avro/Metric.avsc*.
Tis is done by the avro maven plugin and can be achieved running:

```$ mvn clean package -DskipTests```

Zookeeper, Kafka and the Schema Registry have to be running:

```
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties &

$ ./bin/kafka-server-start ./etc/kafka/server.properties &

$ ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties &
```
# Avro generic approach
The AvroGenericSenderTest evaluates a generic approach, where the schema has to be provided with every request. Sending a message with an incompatible schema ends with a SerializationException and the invalid message never reaches the Kafka topic.

# Avro specific approach
In this approach, a schema is provided in form of an Avro schema file, which is translated into a class file, which then can be used to create objects, either via constructors or builders.

# Conclusions and miscellaneous notes
Using the Schema Registry is in that sense transparent, that only the Avro serializers/deserializers have to be put into the Consumer's / Producer's config properties as well as the URL to the Schema Registry.

There is development overhead related to providing the schema, no matter if using the generic nor specific approach. A price to pay for "schema-aware" messages.

There should be a performance decrease since each send request requires the schema to be verified for compatibility. It has been promised to keep it low due to caching on both, the serializer level and Schema Registry level.

By default schema events (register, update) are persisted in a Kafka topic called _schemas.

Confluent ships a command line tool that uses the Schema Registry to inspect messages

```$ ./bin/kafka-avro-console-consumer --from-beginning --zookeeper 127.0.0.1:2181 --topic <topic> --property schema.registry.url=http://127.0.0.1:8081```

A schema is tied to a Kafka topic. Since a Kafka message comes in form of a Key-Value pair, a schema has a suffix, '-key' or '-value' respectively for the Key part and Value part of the message.

The Schema Registry exposes a REST interface, which can be used to evaluate the schema

*Get all schema names (subjects)*

```curl -X GET -i http://localhost:8081/subjects```

*Retrieve all version of a particular schema*

```$ curl -X GET -i http://localhost:8081/subjects/<schema>/versions```

*Retrieve a particular version of a given schema*
```$ curl -X GET -i http://localhost:8081/subjects/<schema>/versions/1```
```$ curl -X GET -i http://localhost:8081/subjects/<schema>/versions/latest```


## Compatibility Level
To check the global compatibility level, which by default is **BACKWARD**, run:

```$ curl -X GET -i http://localhost:8081/config/```

The compatibility level can be set on a per schema basis:

```$ curl -X PUT -i -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "backward"}' http://localhost:8081/config/<schema>```

And listed:

```$ curl -X GET -i http://localhost:8081/config/<schema>```

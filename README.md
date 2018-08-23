[![Build Status](https://travis-ci.org/hortonworks-spark/shc.svg?branch=master)](https://travis-ci.org/hortonworks-spark/spark-schema-registry)

# Apache Spark - Schema Registry integration

The [Apache Spark](https://spark.apache.org/) - [Schema Registry](https://github.com/hortonworks/registry) integration is a library to leverage Schema registry for managing Spark schemas and to serialize/de-serialize messages in spark data sources and sinks.

### Compiling

    mvn clean install

### Running the example programs

The examples can be run from IDE (e.g. Intellij) by specifying a master URL or via spark-submit.

    spark-submit --master <master-url> --class com.hortonworks.spark.registry.examples.SchemaRegistryJsonExample
    spark-schema-registry-examples-0.1-SNAPSHOT.jar <schema-registry-url> <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
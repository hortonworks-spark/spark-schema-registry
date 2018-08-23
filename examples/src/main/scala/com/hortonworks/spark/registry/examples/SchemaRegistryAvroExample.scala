package com.hortonworks.spark.registry.examples

import java.util.UUID

import com.hortonworks.spark.registry.util.SchemaRegistryUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * This example illustrates the usage of Schema registry to de-serialize and serialize
 * messages from and to kafka. See [[SchemaJsonExample]] for the manual approach of
 * specifying the schema as a Spark StructType and [[SchemaRegistryJsonExample]] for
 * using the schema to decode plain JSON messages.
 *
 * This example depends on the resources available at
 * https://github.com/hortonworks/registry/tree/master/examples/schema-registry
 *
 * To run the example:
 * 1. Start a schema registry instance and register the input and output schemas.
 *    E.g. 'topic1' as input schema from below:
 *    https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/resources/truck_events.avsc
 *    and create a subset of fields (driverId, truckId, miles) as the output schema ('topic1-out')
 * 2. Run the Spark application
 *    SchemaRegistryAvroExample <schema-registry-url> <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
 * 3. Ingest sample data using the schema registry example app into input topic
 *    E.g. java -jar avro-examples-*.jar -d data/truck_events_json -p data/kafka-producer.props -sm -s data/truck_events.avsc
 *    (more details - https://github.com/hortonworks/registry/tree/master/examples/schema-registry/avro)
 * 4. Monitor the output topic using [[SchemaRegistryAvroReader]]
 */
object SchemaRegistryAvroExample {

  def main(args: Array[String]): Unit = {

    val schemaRegistryUrl = if (args.length > 0) args(0) else "http://localhost:9090/api/v1/"
    val bootstrapServers = if (args.length > 1) args(1) else "localhost:9092"
    val topic = if (args.length > 2) args(2) else "topic1"
    val outTopic = if (args.length > 3) args(3) else "topic1-out"
    val checkpointLocation =
      if (args.length > 4) args(4) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("SchemaRegistryAvroExample")
      .getOrCreate()

    val messages = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .load()

    import spark.implicits._

    // the schema registry client config
    val config = Map[String, Object]("schema.registry.url" -> schemaRegistryUrl)

    // get an instance of Schema registry util to manage schemas
    val srUtil = SchemaRegistryUtil(spark, config)

    // register a spark UDF to deserialize messages (avro) ingested into
    // the kafka topic using a schema registry de-serializer. This method automatically
    // infers the schema registry schema associated with the topic and maps it to spark schema.
    val from_schema_registry = srUtil.getDeserializer(topic)

    // register a spark UDF to serialize spark rows into
    // the kafka topic using a schema registry serializer.
    val to_schema_registry = srUtil.getSerializer(outTopic)

    // read messages from kafka and deserialize
    val df = messages
      .select(from_schema_registry($"value").alias("message"))

    // project (driverId, truckId, miles) for the events where miles > 300
    val filtered = df.select($"message.driverId", $"message.truckId", $"message.miles")
      .where("message.miles > 300")

    // write the output as schema registry serialized avro records to a kafka topic
    // should produce events like {"driverId":14,"truckId":25,"miles":373}
    val query = filtered
      .select(to_schema_registry(struct($"*")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", outTopic)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(10000))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}

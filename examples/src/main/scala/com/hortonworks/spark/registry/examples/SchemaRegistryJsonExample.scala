package com.hortonworks.spark.registry.examples

import java.util.UUID

import com.hortonworks.spark.registry.util.SchemaRegistryUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * This example illustrates the usage of Schema registry to infer the spark schema
 * based on the kafka topic. See [[SchemaJsonExample]] for the manual approach of
 * specifying the schema as a Spark StructType.
 *
 * This example depends on the resources available at
 * https://github.com/hortonworks/registry/tree/master/examples/schema-registry
 *
 * To run the example:
 * 1. Start a schema registry instance and register the truck events schema with the input kafka topic
 *    name (E.g. topic1) as the schema name.
 *    https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/resources/truck_events.avsc
 * 2. Run the Spark application
 *    SchemaRegistryJsonExample <schema-registry-url> <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
 * 3. Ingest the sample data (json) into input topic
 *    E.g. cat examples/schema-registry/avro/data/truck_events_json | kafka-console-producer.sh
 *    --broker-list host:port --topic topic1
 * 4. Monitor the output topic
 *    E.g. kafka-console-consumer.sh --bootstrap-server host:port --new-consumer --topic topic1-out
 *
 */
object SchemaRegistryJsonExample {

  def main(args: Array[String]): Unit = {

    val schemaRegistryUrl = if (args.length > 0) args(0) else "http://localhost:9090/api/v1/"
    val bootstrapServers = if (args.length > 1) args(1) else "localhost:9092"
    val topic = if (args.length > 2) args(2) else "topic1"
    val outTopic = if (args.length > 3) args(3) else "topic1-out"
    val checkpointLocation =
      if (args.length > 4) args(4) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("SchemaRegistryJsonExample")
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

    // get an instance of schema registry util
    val srUtil = SchemaRegistryUtil(spark, config)

    // retrieve the translated "Spark schema" corresponding to the schema associated
    // with the topic in the schema registry.
    val schema = srUtil.sparkSchema(topic)

    // read messages from kafka and parse it using the above schema
    val df = messages
      .select(from_json($"value".cast("string"), schema).alias("value"))

    // project (driverId, truckId, miles) for the events where miles > 300
    val filtered = df.select($"value.driverId", $"value.truckId", $"value.miles")
      .where("value.miles > 300")

    // write the output to a kafka topic
    // should produce events like {"driverId":14,"truckId":25,"miles":373}
    val query = filtered
      .select(to_json(struct($"*")).alias("value"))
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

package com.hortonworks.spark.registry.examples

import java.util
import java.util.UUID

import com.hortonworks.spark.registry.util.SchemaRegistryUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * This example de-serializes the ouput produced by [[SchemaRegistryAvroExample]] and
 * prints the output to console. The schema is automatically infered by querying the schema
 * registry.
 *
 * Usage:
 * SchemaRegistryAvroReader <schema-registry-url> <bootstrap-servers> <input-topic> <checkpoint-location>
 */
object SchemaRegistryAvroReader {

  def main(args: Array[String]): Unit = {

    val schemaRegistryUrl = if (args.length > 0) args(0) else "http://localhost:9090/api/v1/"
    val bootstrapServers = if (args.length > 1) args(1) else "localhost:9092"
    val topic = if (args.length > 2) args(2) else "topic1-out"
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("SchemaRegistryAvroReader")
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

    // register a spark UDF to deserialize the messages (avro) ingested into
    // the kafka topic. This method automatically infers the schema registry
    // schema associated with the topic and maps it to the equivalent spark schema.
    val from_schema_registry = srUtil.getDeserializer(topic)

    // read messages from kafka and deserialize it using the above UDF
    val df = messages
      .select(from_schema_registry($"value").alias("message"))

    // write the output to console
    // should produce events like {"driverId":14,"truckId":25,"miles":373}
    val query = df
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(10000))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}

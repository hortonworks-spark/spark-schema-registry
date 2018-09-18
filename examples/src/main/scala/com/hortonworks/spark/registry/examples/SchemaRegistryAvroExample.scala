/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.registry.examples

import java.util.UUID

import com.hortonworks.spark.registry.util._
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
 * 1. Start a schema registry instance and register the input schema.
 *    E.g. 'topic1' as input schema from below:
 *    https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/resources/truck_events.avsc
 * 2. Run the Spark application
 *    SchemaRegistryAvroExample <schema-registry-url> <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location> [kafka.security.protocol]
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
    val securityProtocol =
      if (args.length > 5) Option(args(5)) else None

    val spark = SparkSession
      .builder
      .appName("SchemaRegistryAvroExample")
      .getOrCreate()

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)

    val messages = securityProtocol
      .map(p => reader.option("kafka.security.protocol", p).load())
      .getOrElse(reader.load())

    import spark.implicits._

    // the schema registry client config
    val config = Map[String, Object]("schema.registry.url" -> schemaRegistryUrl)

    // the schema registry config that will be implicitly passed
    implicit val srConfig: SchemaRegistryConfig = SchemaRegistryConfig(config)

    // Read messages from kafka and deserialize.
    // This uses the schema registry schema
    // associated with the topic and maps it to spark schema.
    val df = messages
      .select(from_sr($"value", topic).alias("message"))

    // project (driverId, truckId, miles) for the events where miles > 300
    val filtered = df.select($"message.driverId", $"message.truckId", $"message.miles")
      .where("message.miles > 300")

    // write the output as schema registry serialized avro records to a kafka topic
    // should produce events like {"driverId":14,"truckId":25,"miles":373}
    val writer = filtered
      .select(to_sr(struct($"*"), outTopic).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", outTopic)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(10000))
      .outputMode(OutputMode.Append())
    
    val query = securityProtocol
      .map(p => writer.option("kafka.security.protocol", p).start())
      .getOrElse(writer.start())

    query.awaitTermination()
  }

}

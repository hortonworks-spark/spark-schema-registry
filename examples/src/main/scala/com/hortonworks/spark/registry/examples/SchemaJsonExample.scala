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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

/**
 * This example illustrates the usage of Spark StructType to manually define the schema
 * and process messages from kafka. [[SchemaRegistryJsonExample]]
 * and [[SchemaRegistryAvroExample]], illustrates how this can be simplified by
 * integrating with Schema registry.
 *
 * This example depends on the resources available at
 * https://github.com/hortonworks/registry/tree/master/examples/schema-registry
 *
 * To run the example:
 * 1. Run the Spark application
 *    SchemaJsonExample <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
 * 2. Ingest the sample data (json) into input topic
 *    $ cat examples/schema-registry/avro/data/truck_events_json | kafka-console-producer.sh
 *    --broker-list host:port --topic topic1
 * 3. Monitor the output topic
 *    $ kafka-console-consumer.sh --bootstrap-server host:port --new-consumer --topic topic1-out
 *
 */
object SchemaJsonExample {

  def main(args: Array[String]): Unit = {

    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "topic1"
    val outTopic = if (args.length > 2) args(2) else "topic1-out"
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("SchemaExample")
      .getOrCreate()

    val messages = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .load()

    import spark.implicits._

    // the schema for truck events
    val schema = StructType(Seq(
      StructField("driverId", IntegerType, nullable = false),
      StructField("truckId", IntegerType, nullable = false),
      StructField("eventTime", StringType, nullable = false),
      StructField("eventType", StringType, nullable = false),
      StructField("longitude", DoubleType, nullable = false),
      StructField("latitude", DoubleType, nullable = false),
      StructField("eventKey", StringType, nullable = false),
      StructField("correlationId", StringType, nullable = false),
      StructField("driverName", StringType, nullable = false),
      StructField("routeId", IntegerType, nullable = false),
      StructField("routeName", StringType, nullable = false),
      StructField("eventDate", StringType, nullable = false),
      StructField("miles", IntegerType, nullable = false)
    ))

    // read messages from kafka and parse it using the above schema
    val df = messages
      .select(from_json($"value".cast("string"), schema).alias("value"))

    // project (driverId, truckId, miles) for the events where miles > 300
    val filtered = df.select($"value.driverId", $"value.truckId", $"value.miles")
      .where("value.miles > 300")

    // write the output to a kafka topic serialized as a JSON string.
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

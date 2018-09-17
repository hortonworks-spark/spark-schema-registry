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
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * This example de-serializes the ouput produced by [[SchemaRegistryAvroExample]] and
 * prints the output to console. The schema is automatically infered by querying the schema
 * registry.
 *
 * Usage:
 * SchemaRegistryAvroReader <schema-registry-url> <bootstrap-servers> <input-topic> <checkpoint-location> [security.protocol]
 */
object SchemaRegistryAvroReader {

  def main(args: Array[String]): Unit = {

    val schemaRegistryUrl = if (args.length > 0) args(0) else "http://localhost:9090/api/v1/"
    val bootstrapServers = if (args.length > 1) args(1) else "localhost:9092"
    val topic = if (args.length > 2) args(2) else "topic1-out"
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString
    val securityProtocol =
      if (args.length > 4) Option(args(4)) else None

    val spark = SparkSession
      .builder
      .appName("SchemaRegistryAvroReader")
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
    // This uses the schema registry schema associated with the topic.
    val df = messages
      .select(from_sr($"value", topic).alias("message"))

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

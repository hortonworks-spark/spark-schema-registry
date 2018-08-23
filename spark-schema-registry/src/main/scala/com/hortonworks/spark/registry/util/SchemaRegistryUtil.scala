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

package com.hortonworks.spark.registry.util

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import com.hortonworks.registries.schemaregistry.{SchemaMetadata, SchemaVersionInfo, SchemaVersionKey}
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL
import com.hortonworks.registries.schemaregistry.serdes.avro.{AvroSnapshotDeserializer, AvroSnapshotSerializer}
import collection.JavaConverters._

import com.hortonworks.spark.registry.avro.{AvroDeserializer, AvroSerializer, SchemaConverters}
import org.apache.avro.Schema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}

/**
 * Spark schemas and deserialization based on
 * schema registry (https://github.com/hortonworks/registry) schemas.
 *
 * @param session the spark session
 * @param config the schema registry config
 */
class SchemaRegistryUtil(session: SparkSession, config: Map[String, Object]) {
  private val counter: AtomicInteger = new AtomicInteger()
  private val srClient = new SchemaRegistryClient(config.asJava)
  private val registeredUdfs = mutable.Map[(String, Integer), UserDefinedFunction]()

  /**
   * Returns the spark schema (Datatype) corresponding to the latest version of
   * schema defined in the schema registry.
   *
   * @param schemaName the schema name
   * @return the spark Datatype corresponding to the schema
   */
  def sparkSchema(schemaName: String): DataType = {
    sparkDataType(fetchSchemaVersionInfo(schemaName, Option.empty))
  }

  /**
   * Returns the spark schema (Datatype) corresponding to the given version of
   * schema defined in the schema registry.
   *
   * @param schemaName the schema name
   * @param version the schema version
   * @return the spark Datatype corresponding to the schema
   */

  def sparkSchema(schemaName: String, version: Int): DataType = {
    sparkDataType(fetchSchemaVersionInfo(schemaName, Option(version)))
  }

  /**
   * Returns a schema (String) for the given (schema, version)
   * from schema registry. E.g. If the schema type stored in
   * schema registry is Avro, this would return the schema as an Avro string.
   *
   * @param schemaName the schema name
   * @param version    the schema version
   * @return the schema string
   */
  def schemaText(schemaName: String, version: Int): String = {
    fetchSchemaVersionInfo(schemaName, Option(version)).getSchemaText
  }

  /**
   * Registers and returns a Spark UDF for de-serializing values using
   * a schema defined in the schema registry. This
   * will use the writer schema version for reading the values.
   *
   * @param schemaName the schema name (registered in schema registry)
   * @return a Spark user defined function
   */
  def getDeserializer(schemaName: String): UserDefinedFunction = {
    getOrRegisterDeser(schemaName, Option.empty)
  }

  /**
   * Registers and returns a Spark UDF for de-serializing values using
   * a schema defined in the schema registry.
   *
   * @param schemaName the schema name (registered in schema registry)
   * @param version the schema version
   * @return a Spark user defined function
   */
  def getDeserializer(schemaName: String, version: Int): UserDefinedFunction = {
    getOrRegisterDeser(schemaName, Option(version))
  }

  /**
   * Registers and returns a Spark UDF for serializing spark rows using
   * a schema defined in the schema registry. This
   * will use the latest schema version for serializing the values.
   *
   * @param schemaName the schema name (registered in schema registry)
   * @return a Spark user defined function
   */
  def getSerializer(schemaName: String): UserDefinedFunction = {
    getOrRegisterSer(schemaName, Option.empty)
  }

  /**
   * Registers and returns a Spark UDF for serializing spark rows using
   * a schema defined in the schema registry.
   *
   * @param schemaName the schema name (registered in schema registry)
   * @param version the schema version
   * @return a Spark user defined function
   */
  def getSerializer(schemaName: String, version: Int): UserDefinedFunction = {
    getOrRegisterSer(schemaName, Option(version))
  }

  private sealed abstract class Operation(val name: String)
  private case object Deserialize extends Operation("deserialize")
  private case object Serialize extends Operation("serialize")

  private def sparkDataType(srSchema: SchemaVersionInfo): DataType = {
    SchemaRegistryUtil.toSqlType(srSchema)._2
  }

  private def fetchSchemaVersionInfo(
      schemaName: String,
      version: Option[Integer]): SchemaVersionInfo = {
      version.map(v => srClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v)))
        .getOrElse(srClient.getLatestSchemaVersionInfo(schemaName))
  }

  private def getOrRegisterDeser(
      schemaName: String,
      version: Option[Integer]): UserDefinedFunction = {
    registeredUdfs.getOrElseUpdate((schemaName, version.orNull), doRegister(schemaName, version, Deserialize))
  }

  private def getOrRegisterSer(
      schemaName: String,
      version: Option[Integer]): UserDefinedFunction = {
    registeredUdfs.getOrElseUpdate((schemaName, version.orNull), doRegister(schemaName, version, Serialize))
  }

  private def doRegister(
      schemaName: String,
      version: Option[Integer],
      op: Operation): UserDefinedFunction = {
    val udfName = op.name + counter.incrementAndGet()
    val srSchema = fetchSchemaVersionInfo(schemaName, version)
    val (fn, dataType) = op match {
      case Serialize =>
        val schemaMetadata = srClient.getSchemaMetadataInfo(schemaName).getSchemaMetadata
        (new SerImpl(config, srSchema, version, schemaMetadata), BinaryType)
      case Deserialize =>
        (new DeserImpl(config, srSchema, version), SchemaRegistryUtil.toSqlType(srSchema)._2)
    }
    session.udf.register(udfName, fn, dataType)
    udf(fn, dataType)
  }
}

/**
 * Serializes spark Row and returns bytes
 *
 * This implements a Java interface because the sparks UDF registration
 * mechanism allows explicitly specifying spark data type only with Java UDFs.
 */
class SerImpl(
    config: Map[String, Object],
    srSchema: SchemaVersionInfo,
    version: Option[Integer],
    metadata: SchemaMetadata)
  extends UDF1[Object, Array[Byte]] {

  private lazy val ser: AvroSnapshotSerializer = {
    val obj = new AvroSnapshotSerializer()
    obj.init(config.asJava)
    obj
  }

  // a function to convert a Spark Row to avro input format for SR
  private lazy val sparkToAvro: Object => Object = {
    val (avroSchema, dataType) = SchemaRegistryUtil.toSqlType(srSchema)
    val avroSerializer = new AvroSerializer(dataType, avroSchema, false)
    dataType match {
      case structType: StructType =>
        val encoder = RowEncoder.apply(structType).resolveAndBind()
        (o: Object) => avroSerializer.serialize(encoder.toRow(o.asInstanceOf[Row])).asInstanceOf[Object]
      case _ =>
        (o: Object) => avroSerializer.serialize(o).asInstanceOf[Object]
    }

  }

  // returns SR serialized bytes
  override def call(input: Object): Array[Byte] = {
    ser.serialize(sparkToAvro(input), metadata)
  }
}


/**
 * De-serializes bytes and returns a spark Row.
 *
 * This implements a Java interface because the sparks UDF registration
 * mechanism allows explicitly specifying spark data type only with Java UDFs.
 */
class DeserImpl(
    config: Map[String, Object],
    srSchema: SchemaVersionInfo,
    version: Option[Integer])
  extends UDF1[Array[Byte], Object] {

  private lazy val deser: AvroSnapshotDeserializer = {
    val obj = new AvroSnapshotDeserializer()
    obj.init(config.asJava)
    obj
  }

  // a function to convert the SR de-serialized object to Spark Row
  private lazy val avroToSpark: Object => Object = {
    val (avroSchema, dataType) = SchemaRegistryUtil.toSqlType(srSchema)
    val avroDeserializer = new AvroDeserializer(avroSchema, dataType)
    dataType match {
      case structType: StructType =>
        val encoder = RowEncoder.apply(structType).resolveAndBind()
        o: Object => encoder.fromRow(avroDeserializer.deserialize(o).asInstanceOf[InternalRow])
      case _ =>
        o: Object => avroDeserializer.deserialize(o).asInstanceOf[Object]
    }
  }

  // de-serializes SR serialized bytes and returns spark object
  override def call(bytes: Array[Byte]): Object = {
    avroToSpark(deser.deserialize(new ByteArrayInputStream(bytes), version.orNull))
  }
}

/**
 * Factories for building SchemaRegistryUtil
 */
object SchemaRegistryUtil {
  /**
   * Returns an instance of SchemaRegistryUtil. The config should contain "schema.registry.url" that
   * points to a running instance of Schema Registry.
   */
  def apply(session: SparkSession, config: Map[String, Object]): SchemaRegistryUtil =
    new SchemaRegistryUtil(session, config)

  /**
   * Returns an instance of SchemaRegistryUtil that connects to a local Schema Registry instance
   * running at port 9090.
   */
  def apply(session: SparkSession): SchemaRegistryUtil = {
    val config = Map[String, Object](SCHEMA_REGISTRY_URL.name() -> "http://localhost:9090/api/v1/")
    apply(session, config)
  }

  def toSqlType(srSchema: SchemaVersionInfo): (Schema, DataType) = {
    val avroSchema = new Schema.Parser().parse(srSchema.getSchemaText)
    (avroSchema, SchemaConverters.toSqlType(avroSchema).dataType)
  }
}


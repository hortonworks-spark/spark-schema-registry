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

package com.hortonworks.spark.registry

import com.hortonworks.registries.schemaregistry.{SchemaVersionInfo, SchemaVersionKey}
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.spark.registry.avro._
import org.apache.avro.Schema
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

/**
 * Spark schemas and deserialization based on
 * schema registry (https://github.com/hortonworks/registry) schemas.
 */
package object util {

  // wrapper class for Schema registry config
  case class SchemaRegistryConfig(config: Map[String, Object])

  /**
   * Returns the spark schema (Datatype) corresponding to the latest version of
   * schema defined in the schema registry.
   *
   * @param schemaName the schema name
   * @return the spark Datatype corresponding to the schema
   */
  def sparkSchema(schemaName: String)(implicit srConfig: SchemaRegistryConfig): DataType = {
    sparkDataType(fetchSchemaVersionInfo(schemaName, Option.empty, srConfig.config))
  }

  /**
   * Returns the spark schema (Datatype) corresponding to the given version of
   * schema defined in the schema registry.
   *
   * @param schemaName the schema name
   * @param version the schema version
   * @return the spark Datatype corresponding to the schema
   */

  def sparkSchema(schemaName: String, version: Int)(implicit srConfig: SchemaRegistryConfig): DataType = {
    sparkDataType(fetchSchemaVersionInfo(schemaName, Option(version), srConfig.config))
  }

  /**
   * Returns a schema (String) for the latest version of the schema
   * from schema registry. E.g. If the schema type stored in
   * schema registry is Avro, this would return the schema as an Avro string.
   *
   * @param schemaName the schema name
   * @return the schema string
   */
  def schemaText(schemaName: String)(implicit srConfig: SchemaRegistryConfig): String = {
    fetchSchemaVersionInfo(schemaName, Option.empty, srConfig.config).getSchemaText
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
  def schemaText(schemaName: String, version: Int)(implicit srConfig: SchemaRegistryConfig): String = {
    fetchSchemaVersionInfo(schemaName, Option(version), srConfig.config).getSchemaText
  }

  /**
   * Converts schema registry binary format to spark column. This uses the
   * latest version of the schema.
   *
   * @param data       schema registry serialized bytes
   * @param schemaName schema name
   */
  def from_sr(data: Column, schemaName: String)(implicit srConfig: SchemaRegistryConfig): Column = {
    new Column(AvroDataToCatalyst(data.expr, schemaName, Option.empty, srConfig.config))
  }

  /**
   * Converts schema registry binary format to spark column.
   *
   * @param data       schema registry serialized bytes
   * @param schemaName schema name
   * @param version    schema version
   */
  def from_sr(data: Column, schemaName: String, version: Int)(implicit srConfig: SchemaRegistryConfig): Column = {
    new Column(AvroDataToCatalyst(data.expr, schemaName, Option(version), srConfig.config))
  }

  /**
   * Converts a spark column data to binary format of schema registry.
   * This looks up a schema registry schema for the `schemaName` that matches the input
   * and automatically registers a new schema if not found.
   *
   * @param data       the data column
   * @param schemaName the schema registry schema name
   */
  def to_sr(
      data: Column,
      schemaName: String,
      topLevelRecordName: String = "",
      namespace: String = "com.hortonworks.registries")
      (implicit srConfig: SchemaRegistryConfig): Column = {
    new Column(CatalystDataToAvro(data.expr, schemaName, topLevelRecordName, namespace, srConfig.config))
  }

  private def sparkDataType(srSchema: SchemaVersionInfo): DataType = {
    val avroSchema = new Schema.Parser().parse(srSchema.getSchemaText)
    SchemaConverters.toSqlType(avroSchema).dataType
  }

  private def fetchSchemaVersionInfo(
      schemaName: String,
      version: Option[Integer],
      config: Map[String, Object]): SchemaVersionInfo = {
      val srClient = new SchemaRegistryClient(config.asJava)
      version.map(v => srClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v)))
        .getOrElse(srClient.getLatestSchemaVersionInfo(schemaName))
  }
}


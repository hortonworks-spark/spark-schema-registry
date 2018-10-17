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
package com.hortonworks.spark.registry.avro

import java.io.ByteArrayInputStream

import com.hortonworks.registries.schemaregistry.{SchemaVersionInfo, SchemaVersionKey}
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

import scala.collection.JavaConverters._

/**
 * Note: This is a modified version of
 * https://github.com/apache/spark/blob/master/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroDataToCatalyst.scala
 * that users schema registry de-serialization.
 */
case class AvroDataToCatalyst(child: Expression, schemaName: String, version: Option[Int], config: Map[String, Object])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes = Seq(BinaryType)

  @transient private lazy val srDeser: AvroSnapshotDeserializer = {
    val obj = new AvroSnapshotDeserializer()
    obj.init(config.asJava)
    obj
  }

  @transient private lazy val srSchema = fetchSchemaVersionInfo(schemaName, version)

  @transient private lazy val avroSchema = new Schema.Parser().parse(srSchema.getSchemaText)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  @transient private lazy val avroDeser= new AvroDeserializer(avroSchema, dataType)

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    val row = avroDeser.deserialize(srDeser.deserialize(new ByteArrayInputStream(binary), srSchema.getVersion))
    val result = row match {
      case r: InternalRow => r.copy()
      case _ => row
    }
    result
  }

  override def simpleString: String = {
    s"from_sr(${child.sql}, ${dataType.simpleString})"
  }

  override def sql: String = {
    s"from_sr(${child.sql}, ${dataType.catalogString})"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${ctx.boxedType(dataType)})$expr.nullSafeEval($input)")
  }

  private def fetchSchemaVersionInfo(schemaName: String, version: Option[Int]): SchemaVersionInfo = {
    val srClient = new SchemaRegistryClient(config.asJava)
    version.map(v => srClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v)))
      .getOrElse(srClient.getLatestSchemaVersionInfo(schemaName))
  }

}
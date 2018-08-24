package com.hortonworks.spark.registry.util

import java.io.{ByteArrayInputStream, File}

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.hortonworks.registries.schemaregistry.{SchemaCompatibility, SchemaMetadata, SchemaVersion}
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.{AvroSnapshotDeserializer, AvroSnapshotSerializer}
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

case class Truck(driverId: Long, driverName: String, miles: Long)

class SchemaRegistryUtilSuite extends FunSuite with BeforeAndAfterAll {

  var localSchemaRegistryServer: LocalSchemaRegistryServer = _
  var registryConfig: Map[String, Object] = _
  var truckEventsV1Schema: String = _
  var truckEventsV2Schema: String = _
  var srClient: SchemaRegistryClient = _
  var schemaMetadata: SchemaMetadata = _

  override protected def beforeAll(): Unit = {
    val configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI).getAbsolutePath
    val localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath)
    localSchemaRegistryServer.start()
    val srUrl = localSchemaRegistryServer.getLocalURL + "api/v1"
    registryConfig = Map(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name -> srUrl)
    srClient = new SchemaRegistryClient(registryConfig.asJava)
    schemaMetadata = new SchemaMetadata.Builder("truck_events").
      `type`(AvroSchemaProvider.TYPE)
      .schemaGroup("sample-group")
      .description("Sample schema")
      .compatibility(SchemaCompatibility.BACKWARD).build
    srClient.addSchemaMetadata(schemaMetadata)
    truckEventsV1Schema = Resources.toString(Resources.getResource("truck_events-v1.avsc"), Charsets.UTF_8)
    srClient.addSchemaVersion(schemaMetadata, new SchemaVersion(truckEventsV1Schema, "v1"))
    truckEventsV2Schema = Resources.toString(Resources.getResource("truck_events-v2.avsc"), Charsets.UTF_8)
    srClient.addSchemaVersion(schemaMetadata, new SchemaVersion(truckEventsV2Schema, "v2"))
  }


  test("test schema text - latest version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val srUtil = SchemaRegistryUtil(spark, registryConfig)
    val schemaText = srUtil.schemaText("truck_events")
    assert(truckEventsV2Schema == schemaText)
  }

  test("test schema text - specific version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val srUtil = SchemaRegistryUtil(spark, registryConfig)
    val schemaText = srUtil.schemaText("truck_events", 1)
    assert(truckEventsV1Schema == schemaText)
  }


  test("test spark schema - latest version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val srUtil = SchemaRegistryUtil(spark, registryConfig)
    val sparkSchema = srUtil.sparkSchema("truck_events")
    assert(sparkSchemaV2 == sparkSchema)
  }

  test("test spark schema - specific version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val srUtil = SchemaRegistryUtil(spark, registryConfig)
    val sparkSchema = srUtil.sparkSchema("truck_events", 1)
    assert(sparkSchemaV1 == sparkSchema)
  }

  test("test serialize") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val trucks = Seq(
      Truck(1, "driver_1", 100),
      Truck(2, "driver_2", 200),
      Truck(3, "driver_3", 300),
      Truck(4, "driver_4", 400),
      Truck(5, "driver_5", 500)
    )

    val srUtil = SchemaRegistryUtil(spark, registryConfig)
    val to_sr = srUtil.getSerializer("truck_events", 1)

    // serialize the rows using spark
    import spark.implicits._
    val rows = trucks.toDF()
      .select(to_sr(struct($"*")).alias("events"))
      .collect().map(r => r.get(0).asInstanceOf[Array[Byte]]).toList

    // de-serialize the rows using SR deserializer
    val srDeser = new AvroSnapshotDeserializer()
    srDeser.init(registryConfig.asJava)
    val result = rows.map(row => srDeser.deserialize(new ByteArrayInputStream(row), 1))
      .map(r => r.asInstanceOf[GenericData.Record])
      .map(t => Truck(
        t.get("driverId").asInstanceOf[Long],
        t.get("driverName").asInstanceOf[Utf8].toString,
        t.get("miles").asInstanceOf[Long]))

    assert(trucks == result)
  }

  test("test de-serialize") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val trucks = Seq(
      Truck(1, "driver_1", 100),
      Truck(2, "driver_2", 200),
      Truck(3, "driver_3", 300),
      Truck(4, "driver_4", 400),
      Truck(5, "driver_5", 500)
    )

    val srUtil = SchemaRegistryUtil(spark, registryConfig)

    // serialize the rows using SR deserializer
    val srSer = new AvroSnapshotSerializer()
    srSer.init(registryConfig.asJava)
    val serialized = trucks.map(t => {
      val avroRec = new GenericData.Record(new Schema.Parser().parse(truckEventsV1Schema))
      avroRec.put("driverId", t.driverId)
      avroRec.put("driverName", t.driverName)
      avroRec.put("miles", t.miles)
      srSer.serialize(avroRec, schemaMetadata)
    }).asInstanceOf[List[Array[Byte]]]

    // de-serialize using spark
    import spark.implicits._
    val from_sr = srUtil.getDeserializer("truck_events", 1)
    val result = serialized.toDF("value")
      .select(from_sr($"value").alias("t"))
      .select($"t.driverId", $"t.driverName", $"t.miles")
      .as[Truck].collect().toList
    assert(trucks == result)
  }


  private val sparkSchemaV1 = StructType(Seq(
    StructField("driverId", LongType, nullable = false),
    StructField("driverName", StringType, nullable = false),
    StructField("miles", LongType, nullable = false)
  ))

  private val sparkSchemaV2 = StructType(Seq(
    StructField("driverId", LongType, nullable = false),
    StructField("driverName", StringType, nullable = false),
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false),
    StructField("miles", LongType, nullable = false)
  ))

}

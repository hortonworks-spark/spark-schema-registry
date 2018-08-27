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
import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

case class Truck(driverId: Long, driverName: String, miles: Long)
case class TruckV2(driverId: Long, driverName: String, miles: Long, latitude: Double, longitude: Double)

class SchemaRegistryUtilSuite extends FunSuite with BeforeAndAfterAll {

  var localSchemaRegistryServer: LocalSchemaRegistryServer = _
  implicit var registryConfig: SchemaRegistryConfig = _
  var trucksV1Schema: String = _
  var trucksV2Schema: String = _
  var numbersSchema: String = _
  var srClient: SchemaRegistryClient = _
  var trucksSchemaMetadata: SchemaMetadata = _
  var numbersSchemaMetadata: SchemaMetadata = _

  override protected def beforeAll(): Unit = {
    val configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI).getAbsolutePath
    val localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath)
    localSchemaRegistryServer.start()
    val srUrl = localSchemaRegistryServer.getLocalURL + "api/v1"
    registryConfig = SchemaRegistryConfig(Map(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name -> srUrl))
    srClient = new SchemaRegistryClient(registryConfig.config.asJava)
    trucksSchemaMetadata = new SchemaMetadata.Builder("trucks").
      `type`(AvroSchemaProvider.TYPE)
      .schemaGroup("trucks-group")
      .description("Trucks schema")
      .compatibility(SchemaCompatibility.BACKWARD).build
    numbersSchemaMetadata = new SchemaMetadata.Builder("numbers").
      `type`(AvroSchemaProvider.TYPE)
      .schemaGroup("numbers-group")
      .description("Numbers schema")
      .compatibility(SchemaCompatibility.BACKWARD).build
    srClient.addSchemaMetadata(trucksSchemaMetadata)
    trucksV1Schema = Resources.toString(Resources.getResource("trucks-v1.avsc"), Charsets.UTF_8)
    srClient.addSchemaVersion(trucksSchemaMetadata, new SchemaVersion(trucksV1Schema, "v1"))
    trucksV2Schema = Resources.toString(Resources.getResource("trucks-v2.avsc"), Charsets.UTF_8)
    srClient.addSchemaVersion(trucksSchemaMetadata, new SchemaVersion(trucksV2Schema, "v2"))
    numbersSchema = Resources.toString(Resources.getResource("numbers.avsc"), Charsets.UTF_8)
    srClient.addSchemaVersion(numbersSchemaMetadata, new SchemaVersion(numbersSchema, "v1"))
  }


  test("test schema text - latest version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()
    val schema = schemaText("trucks")
    assert(trucksV2Schema == schema)
  }

  test("test schema text - specific version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()
    val schema = schemaText("trucks", 1)
    assert(trucksV1Schema == schema)
  }


  test("test spark schema - latest version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()
    val schema = sparkSchema("trucks")
    assert(sparkSchemaV2 == schema)
  }

  test("test spark schema - specific version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()
    val schema = sparkSchema("trucks", 1)
    assert(sparkSchemaV1 == schema)
  }

  test("test to_sr") {
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

    // serialize the rows using spark
    import spark.implicits._
    val rows = trucks.toDF()
      .select(to_sr(struct($"*"), "events").alias("events"))
      .collect().map(r => r.get(0).asInstanceOf[Array[Byte]]).toList

    // SR should register the 'events' schema automatically
    val versions = srClient.getAllVersions("events")
    assert(versions.size() == 1)

    // de-serialize the rows using SR deserializer
    val srDeser = new AvroSnapshotDeserializer()
    srDeser.init(registryConfig.config.asJava)
    val result = rows.map(row => srDeser.deserialize(new ByteArrayInputStream(row), 1))
      .map(r => r.asInstanceOf[GenericData.Record])
      .map(t => Truck(
        t.get("driverId").asInstanceOf[Long],
        t.get("driverName").asInstanceOf[Utf8].toString,
        t.get("miles").asInstanceOf[Long]))

    assert(trucks == result)

  }

  test("test to_sr with registered schema") {
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

    // serialize the rows using spark
    import spark.implicits._
    val rows = trucks.toDF()
      .select(to_sr(struct($"*"), "trucks", "trucks").alias("events"))
      .collect().map(r => r.get(0).asInstanceOf[Array[Byte]]).toList

    // de-serialize the rows using SR deserializer
    val srDeser = new AvroSnapshotDeserializer()
    srDeser.init(registryConfig.config.asJava)
    val result = rows.map(row => srDeser.deserialize(new ByteArrayInputStream(row), 1))
      .map(r => r.asInstanceOf[GenericData.Record])
      .map(t => Truck(
        t.get("driverId").asInstanceOf[Long],
        "driver_" + t.get("driverId").asInstanceOf[Long],
        t.get("miles").asInstanceOf[Long]))

    assert(trucks == result)

    val versions = srClient.getAllVersions("trucks")

    assert(versions.size() == 2)
  }


  test("test from_sr specific version") {
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

    // serialize the rows using SR serializer
    val srSer = new AvroSnapshotSerializer()
    srSer.init(registryConfig.config.asJava)
    val serialized = trucks.map(t => {
      val avroRec = new GenericData.Record(new Schema.Parser().parse(trucksV1Schema))
      avroRec.put("driverId", t.driverId)
      avroRec.put("driverName", t.driverName)
      avroRec.put("miles", t.miles)
      srSer.serialize(avroRec, trucksSchemaMetadata)
    }).asInstanceOf[List[Array[Byte]]]

    // de-serialize using spark
    import spark.implicits._
    val result = serialized.toDF("value")
      .select(from_sr($"value", "trucks", 1).alias("t"))
      .select($"t.driverId", $"t.driverName", $"t.miles")
      .as[Truck].collect().toList

    assert(trucks == result)
  }


  test("test from_sr latest version") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val trucks = Seq(
      TruckV2(1, "driver_1", 100, 1.0, 1.0),
      TruckV2(2, "driver_2", 200, 2.0, 2.0),
      TruckV2(3, "driver_3", 300, 3.0, 3.0),
      TruckV2(4, "driver_4", 400, 4.0, 4.0),
      TruckV2(5, "driver_5", 500, 5.0, 5.0)
    )

    // serialize the rows using SR serializer
    val srSer = new AvroSnapshotSerializer()
    srSer.init(registryConfig.config.asJava)
    val serialized = trucks.map(t => {
      val avroRec = new GenericData.Record(new Schema.Parser().parse(trucksV2Schema))
      avroRec.put("driverId", t.driverId)
      avroRec.put("driverName", t.driverName)
      avroRec.put("miles", t.miles)
      avroRec.put("latitude", t.latitude)
      avroRec.put("longitude", t.longitude)
      srSer.serialize(avroRec, trucksSchemaMetadata)
    }).asInstanceOf[List[Array[Byte]]]

    // de-serialize using spark
    import spark.implicits._
    val result = serialized.toDF("value")
      .select(from_sr($"value", "trucks").alias("t"))
      .select($"t.driverId", $"t.driverName", $"t.miles", $"t.latitude", $"t.longitude")
      .as[TruckV2].collect().toList

    assert(trucks == result)
  }

  test("test primitive ser") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    // serialize the rows using spark
    import spark.implicits._
    val numbers = List(1, 2, 3, 4, 5)
    val rows = numbers.toDF("value")
      .select(to_sr($"value", "numbers").alias("numbers"))
      .collect().map(r => r.get(0).asInstanceOf[Array[Byte]]).toList

    // SR should register the 'numbers' schema automatically
    val versions = srClient.getAllVersions("numbers")
    assert(versions.size() == 1)

    // de-serialize the rows using SR deserializer
    val srDeser = new AvroSnapshotDeserializer()
    srDeser.init(registryConfig.config.asJava)
    val result = rows.map(row => srDeser.deserialize(new ByteArrayInputStream(row), 1))
      .map(r => r.asInstanceOf[Int])
    assert(numbers == result)
  }

  test("test primitive deser") {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .getOrCreate()

    val numbers = List(1, 2, 3, 4, 5)
    // serialize the rows using SR serializer
    val srSer = new AvroSnapshotSerializer()
    srSer.init(registryConfig.config.asJava)
    val serialized = numbers.map(n => srSer.serialize(Integer.valueOf(n), numbersSchemaMetadata))
      .asInstanceOf[List[Array[Byte]]]

    // de-serialize using spark
    import spark.implicits._
    val result = serialized.toDF("value")
      .select(from_sr($"value", "numbers").alias("n"))
      .as[Int].collect().toList

    assert(numbers == result)
  }

  private val sparkSchemaV1 = StructType(Seq(
    StructField("driverId", LongType, nullable = false),
    StructField("driverName", StringType, nullable = true),
    StructField("miles", LongType, nullable = false)
  ))

  private val sparkSchemaV2 = StructType(Seq(
    StructField("driverId", LongType, nullable = false),
    StructField("driverName", StringType, nullable = true),
    StructField("miles", LongType, nullable = false),
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false)
  ))

}

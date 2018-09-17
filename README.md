[![Build Status](https://travis-ci.org/hortonworks-spark/spark-schema-registry.svg?branch=master)](https://travis-ci.org/hortonworks-spark/spark-schema-registry)

# Apache Spark - Schema Registry integration

The [Apache Spark](https://spark.apache.org/) - [Schema Registry](https://github.com/hortonworks/registry) integration is a library to 
leverage Schema registry for managing Spark schemas and to serialize/de-serialize messages in spark data sources and sinks.

### Compiling

    mvn clean package

### Running the example programs

The [examples](examples/src/main/scala/com/hortonworks/spark/registry/examples/) illustrates the API usage and how to integrate with schema registry. 
The examples can be run from IDE (e.g. Intellij) by specifying a master URL or via spark-submit.

    spark-submit --master <master-url> \
    --jars  spark-schema-registry-0.1-SNAPSHOT-jar-with-dependencies.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --class com.hortonworks.spark.registry.examples.<classname> \
    spark-schema-registry-examples-0.1-SNAPSHOT.jar <schema-registry-url> \
    <bootstrap-servers> <input-topic> <output-topic> <checkpoint-location>
    
### Using the APIs

Typically in a spark application you need to define the spark schema for the data you are going to process.

```scala
// the schema for truck events
val schema = StructType(Seq(
  StructField("driverId", IntegerType, nullable = false),
  StructField("truckId", IntegerType, nullable = false),
  StructField("miles", LongType, nullable = false),
  StructField("eventType", StringType, nullable = false),
  ...
  ...
)

// read Json string messages from the data source
val messages = spark
      .readStream
      .format(...)
      .option(...)
      .load()  
              
// parse the messages using the above schema and do further operations
val df = messages
      .select(from_json($"value".cast("string"), schema).alias("value"))
      ...      

// project (driverId, truckId, miles) for the events where miles > 300
val filtered = df.select($"value.driverId", $"value.truckId", $"value.miles")
      .where("value.miles > 300")
```

The above approach is brittle since the schema information is tightly coupled with the code. The code needs to be changed 
when the schema changes. Also there is no sharing or re-use of schema between the message producers and the applications
that wishes to consume the messages. Schema registry also allows you to manage different versions of the schema and define
compatibility policies.

#### Configuration

The Schema registry integration comes as utility methods which can be imported into the scope.

```scala
import com.hortonworks.spark.registry.util._
```

Before invoking the APIs, you need to define an implicit `SchemaRegistryConfig` which will be passed to the APIs. The main 
configuration here is the schema registry URL.

```scala
// the schema registry client config
val config = Map[String, Object]("schema.registry.url" -> schemaRegistryUrl)

// the schema registry config that will be implicitly passed
implicit val srConfig:SchemaRegistryConfig = SchemaRegistryConfig(config)
```
#### Fetching spark schema by name

The API supports fetching the Schema Registry schema as a Spark schema.


- `sparkSchema(schemaName: String)`

   Returns the spark schema corresponding to the latest version of schema defined in the schema registry.
    
- `sparkSchema(schemaName: String, version: Int)`

   Returns the spark schema corresponding to the given version of schema defined in the schema registry.
    
The example discussed above can by simplified as follows without having to explicitly specify the spark schema
in the code.
  
```scala
// retrieve the translated "Spark schema" by specifying the schema registry schema name
val schema = sparkSchema(name)

// parse the messages using the above schema and do further operations
val df = messages
         .select(from_json($"value".cast("string"), schema).alias("value"))
         ...
               
// project (driverId, truckId, miles) for the events where miles > 300
val filtered = df.select($"value.driverId", $"value.truckId", $"value.miles")
                  .where("value.miles > 300")

```    
    
#### Serializing messages using schema registry

The following method can be used to serialize the messages from spark to schema registry binary format 
using schema registry serializers.

- `to_sr(data: Column, schemaName: String, topLevelRecordName: String, namespace: String)`

   Converts a spark column data to binary format of schema registry. This looks up a schema registry schema
   for the `schemaName` that matches the input and automatically registers a new schema if not found.
   The `topoLevelRecordName` and `namespace` are optional and will be mapped to avro top level record name
   and record namespace.

#### De-serializing messages using schema registry

The following methods can be used to de-serialize schema registry serialized messages into spark columns.

-  `from_sr(data: Column, schemaName: String)`

    Converts schema registry binary format to spark column. This uses the latest version of the schema.
    
- `from_sr(data: Column, schemaName: String, version: Int)`

    Converts schema registry binary format to spark column using the given schema registry schema name and version. 
  
#### Serialization - deserialization example

Here is an example that uses the `from_sr` to de-serialize schema registry formatted messages into spark,
transforms and serializes it back to schema registry format using `to_sr` and writes to a data sink. 

This example assumes Spark structured streaming, but this should work well for the non-streaming use cases as well (read and write). 
 
```scala
// Read schema registry formatted messages and deserialize to spark columns.
val df = messages
      .select(from_sr($"value", topic).alias("message"))

// project (driverId, truckId, miles) for the events where miles > 300
val filtered = df.select($"message.driverId", $"message.truckId", $"message.miles")
      .where("message.miles > 300")

// write the output as schema registry serialized bytes to a sink
// should produce events like {"driverId":14,"truckId":25,"miles":373}
val query = filtered
      .select(to_sr(struct($"*"), outSchemaName).alias("value"))
      .writeStream
      .format(..)
      .start()
```       

The output schema `outSchemaName` would be automatically published to schema registry if it does not exist.

### Building and deploying your app

Add a maven dependency in your project to make use of the library and build your application jar.

     <dependency>
        <groupId>com.hortonworks</groupId>
        <artifactId>spark-schema-registry</artifactId>
        <version>0.1-SNAPSHOT</version>
     </dependency>

Once the application jar is built, you can deploy it by adding the dependency in spark-submit via `--packages`

    spark-submit --master <master-url> \
    --packages com.hortonworks:spark-schema-registry:0.1-SNAPSHOT \
    --class YourApp \
    your-application-jar \
    args ...
    
To make it work, you should make sure this package is published in some repositories or exists in your local repository.

If this package is not published to repository or your Spark application cannot access external network, you could use uber jar 
instead, like:

    spark-submit --master <master-url> \
    --jars spark-schema-registry-0.1-SNAPSHOT-jar-with-dependencies.jar \
    --class YourApp \
    your-application-jar \
    args ...

### Running in a Kerberos enabled cluster

The library works in a Kerberos set up where Spark and Schema registry has been deployed on a Kerberos
enabled cluster.

The main thing is to setup the appropriate `JAAS` config for the `RegistryClient` (and `KafkaClient` if the spark 
data source or sink is Kafka). As an example, to run the `SchemaRegistryAvroExample` in a Kerberos set up,

1. Create a keytab (say app.keytab) with the login user and principal you want to run the Application.
2. Create an app_jaas.conf and specify the keytab and principal created in step 1.

   (if deploying to YARN, the keytab and conf files will be distributed as YARN local resources. They will end up 
   in the current directory of the Spark YARN container and the location should be specified as ./app.keytab)

    ```
    RegistryClient {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        keyTab="./app.keytab"
        storeKey=true
        useTicketCache=false
        principal="<principal>";
    };
    
    KafkaClient {
       com.sun.security.auth.module.Krb5LoginModule required
       useKeyTab=true
       keyTab="./app.keytab"
       storeKey=true
       useTicketCache=false
       serviceName="kafka"
       principal="<principal>";
    };
    
    ```
    
4. Provide the required ACLs for the kafka topics (in-topic, out-topic) for the principal.
     
5. In `spark-submit` pass the jaas configuration file via `extraJavaOptions` (and also as local resource files in YARN cluster mode)

   ```
    spark-submit --master yarn --deploy-mode cluster \
       --keytab app.keytab --principal <principal> \
       --files app_jaas.conf#app_jaas.conf,app.keytab#app.keytab \
       --jars spark-schema-registry-0.1-SNAPSHOT-jar-with-dependencies.jar \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
       --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./app_jaas.conf" \
       --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./app_jaas.conf" \
       --class com.hortonworks.spark.registry.examples.SchemaRegistryAvroExample 
       spark-schema-registry-examples-0.1-SNAPSHOT.jar \
       <schema-registry-url> <bootstrap-server> <in-topic> <out-topic> <checkpoint-dir> SASL_PLAINTEXT
   ```

# Spark-Pinot Connector

Spark-pinot connector to read and write data from/to Pinot.

Detailed read model documentation is here; [Spark-Pinot Connector Read Model](documentation/read_model.md)


## Features
- Query realtime, offline or hybrid tables
- Distributed, parallel scan
- Column and filter push down to optimize performance
- Overlap between realtime and offline segments is queried exactly once for hybrid tables
- Schema discovery 
  - Dynamic inference
  - Static analysis of case class

## Quick Start
```scala
import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-pinot-connector-test")
      .master("local")
      .getOrCreate()

import spark.implicits._

val data = spark.read
  .format("pinot")
  .option("table", "airlineStats_OFFLINE")
  .load()
  .filter($"DestStateName" === "Florida")

data.show(100)
```

For more examples, see `src/test/scala/example/ExampleSparkPinotConnectorTest.scala` 

## Future Works
- Add integration tests for read operation
- Add write support(pinot segment write logic will be changed in later versions of pinot)

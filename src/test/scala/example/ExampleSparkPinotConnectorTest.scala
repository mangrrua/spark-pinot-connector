package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Example class to test connector.
  * To run this class, first of all,
  * run pinot locally(https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally)
  */
object ExampleSparkPinotConnectorTest {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-pinot-connector-test")
      .master("local")
      .getOrCreate()

    readOffline()
    readHybrid()
    readHybridWithSpecificSchema()
    readOfflineWithFilters()
    readHybridWithFilters()
    readRealtimeWithSelectionColumns()
  }

  def readOffline()(implicit spark: SparkSession): Unit = {
    println("## readOffline ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats_OFFLINE")
      .load()

    data.show()
  }

  def readHybrid()(implicit spark: SparkSession): Unit = {
    println("## readHybrid ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .load()

    data.show()
  }

  def readHybridWithSpecificSchema()(implicit spark: SparkSession): Unit = {
    println("## readHybridWithSpecificSchema ##")
    val schema = StructType(
      Seq(
        StructField("Distance", DataTypes.IntegerType),
        StructField("AirlineID", DataTypes.IntegerType),
        StructField("DaysSinceEpoch", DataTypes.IntegerType),
        StructField("DestStateName", DataTypes.StringType),
        StructField("Origin", DataTypes.StringType),
        StructField("Carrier", DataTypes.StringType)
      )
    )

    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .schema(schema)
      .load()

    data.show()
  }

  def readOfflineWithFilters()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("## readOfflineWithFilters ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats_OFFLINE")
      .load()
      .filter($"AirlineID" === 19805)
      .filter($"DestStateName" === "Florida")
      .filter($"DaysSinceEpoch".isin(16101, 16084, 16074))
      .filter($"Origin" === "ORD")

    data.show()
  }

  def readHybridWithFilters()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("## readHybridWithFilters ##")
    // should return 1 data, because connector ensure that the overlap
    // between realtime and offline segment data is queried exactly once
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats")
      .load()
      .filter($"AirlineID" === 19805)
      .filter($"DestStateName" === "Florida")
      .filter($"DaysSinceEpoch".isin(16101, 16084, 16074))
      .filter($"Origin" === "ORD")

    data.show()
  }

  def readRealtimeWithSelectionColumns()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println("## readRealtimeWithSelectionColumns ##")
    val data = spark.read
      .format("pinot")
      .option("table", "airlineStats_REALTIME")
      .load()
      .select($"FlightNum", $"Origin", $"DestStateName")

    data.show()
  }

}

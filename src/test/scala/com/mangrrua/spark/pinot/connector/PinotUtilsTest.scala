package com.mangrrua.spark.pinot.connector

import com.mangrrua.spark.pinot.BaseTest
import com.mangrrua.spark.pinot.connector.Constants.PinotTableTypes
import com.mangrrua.spark.pinot.connector.PinotUtils._
import com.mangrrua.spark.pinot.exceptions.PinotException
import org.apache.pinot.common.utils.DataSchema
import org.apache.pinot.common.utils.DataSchema.ColumnDataType
import org.apache.pinot.core.common.datatable.DataTableBuilder
import org.apache.pinot.spi.data.Schema
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.io.Source

class PinotUtilsTest extends BaseTest {
  test("Pinot raw table name should be extracted") {
    val realTimeTableName = "tbl_REALTIME"
    val offlineTableName = "tbl_OFFLINE"
    val hybridTableName = "tbl"

    getRawTableName(realTimeTableName) shouldEqual "tbl"
    getRawTableName(offlineTableName) shouldEqual "tbl"
    getRawTableName(hybridTableName) shouldEqual "tbl"
  }

  test("Pinot table type should be extracted") {
    val realTimeTableName = "tbl_REALTIME"
    val offlineTableName = "tbl_OFFLINE"
    val hybridTableName = "tbl"

    getTableType(realTimeTableName) shouldEqual Some(PinotTableTypes.REALTIME)
    getTableType(offlineTableName) shouldEqual Some(PinotTableTypes.OFFLINE)
    getTableType(hybridTableName) shouldEqual None
  }

  test("Pinot DataTable should be converted to Spark InternalRows") {
    val columnNames = Array(
      "strCol",
      "intCol",
      "longCol",
      "floatCol",
      "doubleCol",
      "strArrayCol",
      "intArrayCol",
      "longArrayCol",
      "floatArrayCol",
      "doubleArrayCol"
    )
    val columnTypes = Array(
      ColumnDataType.STRING,
      ColumnDataType.INT,
      ColumnDataType.LONG,
      ColumnDataType.FLOAT,
      ColumnDataType.DOUBLE,
      ColumnDataType.STRING_ARRAY,
      ColumnDataType.INT_ARRAY,
      ColumnDataType.LONG_ARRAY,
      ColumnDataType.FLOAT_ARRAY,
      ColumnDataType.DOUBLE_ARRAY
    )
    val dataSchema = new DataSchema(columnNames, columnTypes)

    val dataTableBuilder = new DataTableBuilder(dataSchema)
    dataTableBuilder.startRow()
    dataTableBuilder.setColumn(0, "strValueDim")
    dataTableBuilder.setColumn(1, 5)
    dataTableBuilder.setColumn(2, 3L)
    dataTableBuilder.setColumn(3, 10.05f)
    dataTableBuilder.setColumn(4, 2.3d)
    dataTableBuilder.setColumn(5, Array[String]("strArr1", "null"))
    dataTableBuilder.setColumn(6, Array[Int](1, 2, 0))
    dataTableBuilder.setColumn(7, Array[Long](10L, 0))
    dataTableBuilder.setColumn(8, Array[Float](0, 15.20f))
    dataTableBuilder.setColumn(9, Array[Double](0, 10.3d))
    dataTableBuilder.finishRow()
    val dataTable = dataTableBuilder.build()

    val schema = StructType(
      Seq(
        StructField("intArrayCol", ArrayType(IntegerType)),
        StructField("intCol", IntegerType),
        StructField("doubleArrayCol", ArrayType(DoubleType)),
        StructField("doubleCol", DoubleType),
        StructField("strArrayCol", ArrayType(StringType)),
        StructField("longCol", LongType),
        StructField("longArrayCol", ArrayType(LongType)),
        StructField("strCol", StringType),
        StructField("floatArrayCol", ArrayType(FloatType)),
        StructField("floatCol", FloatType)
      )
    )

    val result = pinotDataTableToInternalRows(dataTable, schema).head
    result.getArray(0) shouldEqual ArrayData.toArrayData(Seq(1, 2, 0))
    result.getInt(1) shouldEqual 5
    result.getArray(2) shouldEqual ArrayData.toArrayData(Seq(0d, 10.3d))
    result.getDouble(3) shouldEqual 2.3d
    result.getArray(4) shouldEqual ArrayData.toArrayData(
      Seq("strArr1", "null").map(UTF8String.fromString)
    )
    result.getLong(5) shouldEqual 3L
    result.getArray(6) shouldEqual ArrayData.toArrayData(Seq(10L, 0L))
    result.getString(7) shouldEqual "strValueDim"
    result.getArray(8) shouldEqual ArrayData.toArrayData(Seq(0f, 15.20f))
    result.getFloat(9) shouldEqual 10.05f
  }

  test("Method should throw field not found exception while converting pinot data table") {
    val columnNames = Array("strCol", "intCol")
    val columnTypes = Array(ColumnDataType.STRING, ColumnDataType.INT)
    val dataSchema = new DataSchema(columnNames, columnTypes)

    val dataTableBuilder = new DataTableBuilder(dataSchema)
    dataTableBuilder.startRow()
    dataTableBuilder.setColumn(0, "strValueDim")
    dataTableBuilder.setColumn(1, 5)
    dataTableBuilder.finishRow()
    val dataTable = dataTableBuilder.build()

    val schema = StructType(
      Seq(
        StructField("strCol", StringType),
        StructField("intCol", IntegerType),
        StructField("longCol", LongType)
      )
    )

    val exception = intercept[PinotException] {
      pinotDataTableToInternalRows(dataTable, schema)
    }

    exception.getMessage shouldEqual s"'longCol' not found in Pinot server response"
  }

  test("Pinot schema should be converted to spark schema") {
    val pinotSchemaAsString = Source.fromResource("schema/pinot-schema.json").mkString
    val resultSchema = pinotSchemaToSparkSchema(Schema.fromString(pinotSchemaAsString))
    val sparkSchemaAsString = Source.fromResource("schema/spark-schema.json").mkString
    val sparkSchema = DataType.fromJson(sparkSchemaAsString).asInstanceOf[StructType]
    resultSchema.fields should contain theSameElementsAs sparkSchema.fields
  }
}

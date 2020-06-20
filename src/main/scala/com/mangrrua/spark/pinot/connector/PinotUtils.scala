package com.mangrrua.spark.pinot.connector

import com.mangrrua.spark.pinot.connector.Constants.{PinotTableType, PinotTableTypes}
import com.mangrrua.spark.pinot.exceptions.PinotException
import org.apache.pinot.common.utils.DataSchema.ColumnDataType
import org.apache.pinot.common.utils.DataTable
import org.apache.pinot.spi.data.{FieldSpec, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

private[pinot] object PinotUtils {
  private val OFFLINE_TABLE_SUFFIX = s"_${PinotTableTypes.OFFLINE}"
  private val REALTIME_TABLE_SUFFIX = s"_${PinotTableTypes.REALTIME}"

  /** Extract raw pinot table name. */
  def getRawTableName(tableName: String): String = {
    if (tableName.endsWith(OFFLINE_TABLE_SUFFIX)) {
      tableName.substring(0, tableName.length - OFFLINE_TABLE_SUFFIX.length)
    } else if (tableName.endsWith(REALTIME_TABLE_SUFFIX)) {
      tableName.substring(0, tableName.length - REALTIME_TABLE_SUFFIX.length)
    } else {
      tableName
    }
  }

  /** Return offline/realtime table type, or None if table is hybrid. */
  def getTableType(tableName: String): Option[PinotTableType] = {
    if (tableName.endsWith(OFFLINE_TABLE_SUFFIX)) {
      Some(PinotTableTypes.OFFLINE)
    } else if (tableName.endsWith(REALTIME_TABLE_SUFFIX)) {
      Some(PinotTableTypes.REALTIME)
    } else {
      None
    }
  }

  /** Convert a Pinot schema to Spark schema. */
  def pinotSchemaToSparkSchema(schema: Schema): StructType = {
    val structFields = schema.getAllFieldSpecs.asScala.map { field =>
      val sparkDataType = pinotDataTypeToSparkDataType(field.getDataType)
      if (field.isSingleValueField) {
        StructField(field.getName, sparkDataType)
      } else {
        StructField(field.getName, ArrayType(sparkDataType))
      }
    }.toList
    StructType(structFields)
  }

  private def pinotDataTypeToSparkDataType(dataType: FieldSpec.DataType): DataType =
    dataType match {
      case FieldSpec.DataType.INT => IntegerType
      case FieldSpec.DataType.LONG => LongType
      case FieldSpec.DataType.FLOAT => FloatType
      case FieldSpec.DataType.DOUBLE => DoubleType
      case FieldSpec.DataType.STRING => StringType
      case _ =>
        throw PinotException(s"Unsupported pinot data type '$dataType")
    }

  /** Convert Pinot DataTable to Seq of InternalRow */
  def pinotDataTableToInternalRows(
      dataTable: DataTable,
      sparkSchema: StructType): Seq[InternalRow] = {
    val dataTableColumnNames = dataTable.getDataSchema.getColumnNames
    (0 until dataTable.getNumberOfRows).map { rowIndex =>
      // spark schema is used to ensure columns order
      val columns = sparkSchema.fields.map { field =>
        val colIndex = dataTableColumnNames.indexOf(field.name)
        if (colIndex < 0) {
          throw PinotException(s"'${field.name}' not found in Pinot server response")
        } else {
          // pinot column data type can be used directly,
          // because all of them is supported in spark schema
          val columnDataType = dataTable.getDataSchema.getColumnDataType(colIndex)
          readPinotColumnData(dataTable, columnDataType, rowIndex, colIndex)
        }
      }
      InternalRow.fromSeq(columns)
    }
  }

  private def readPinotColumnData(
      dataTable: DataTable,
      columnDataType: ColumnDataType,
      rowIndex: Int,
      colIndex: Int): Any = columnDataType match {
    // single column types
    case ColumnDataType.STRING =>
      UTF8String.fromString(dataTable.getString(rowIndex, colIndex))
    case ColumnDataType.INT =>
      dataTable.getInt(rowIndex, colIndex)
    case ColumnDataType.LONG =>
      dataTable.getLong(rowIndex, colIndex)
    case ColumnDataType.FLOAT =>
      dataTable.getFloat(rowIndex, colIndex)
    case ColumnDataType.DOUBLE =>
      dataTable.getDouble(rowIndex, colIndex)

    // array column types
    case ColumnDataType.STRING_ARRAY =>
      ArrayData.toArrayData(
        dataTable.getStringArray(rowIndex, colIndex).map(UTF8String.fromString).toSeq
      )
    case ColumnDataType.INT_ARRAY =>
      ArrayData.toArrayData(dataTable.getIntArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.LONG_ARRAY =>
      ArrayData.toArrayData(dataTable.getLongArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.FLOAT_ARRAY =>
      ArrayData.toArrayData(dataTable.getFloatArray(rowIndex, colIndex).toSeq)
    case ColumnDataType.DOUBLE_ARRAY =>
      ArrayData.toArrayData(dataTable.getDoubleArray(rowIndex, colIndex).toSeq)

    case _ =>
      throw PinotException(s"'$columnDataType' is not supported")
  }

}

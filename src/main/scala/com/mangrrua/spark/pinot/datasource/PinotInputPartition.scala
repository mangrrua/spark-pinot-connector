package com.mangrrua.spark.pinot.datasource

import com.mangrrua.spark.pinot.connector.PinotSplit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class PinotInputPartition(
    schema: StructType,
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends InputPartition[InternalRow]
  with Logging {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new PinotInputPartitionReader(schema, partitionId, pinotSplit, dataSourceOptions)
  }
}

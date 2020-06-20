package com.mangrrua.spark.pinot.datasource

import com.mangrrua.spark.pinot.connector.{PinotServerDataFetcher, PinotSplit, PinotUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

class PinotInputPartitionReader(
    schema: StructType,
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends InputPartitionReader[InternalRow] {
  private val responseIterator: Iterator[InternalRow] = fetchDataAndConvertToInternalRows()
  private[this] var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (!responseIterator.hasNext) {
      return false
    }
    currentRow = responseIterator.next()
    true
  }

  override def get(): InternalRow = {
    currentRow
  }

  override def close(): Unit = {}

  private def fetchDataAndConvertToInternalRows(): Iterator[InternalRow] = {
    PinotServerDataFetcher(partitionId, pinotSplit, dataSourceOptions)
      .fetchData()
      .flatMap(PinotUtils.pinotDataTableToInternalRows(_, schema))
      .toIterator
  }
}

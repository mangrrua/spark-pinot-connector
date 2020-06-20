package com.mangrrua.spark.pinot.datasource

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

class PinotDataSourceV2 extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName(): String = "pinot"

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new PinotDataSourceReader(options)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new PinotDataSourceReader(options, Some(schema))
  }
}

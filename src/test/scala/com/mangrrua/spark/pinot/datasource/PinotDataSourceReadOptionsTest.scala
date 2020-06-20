package com.mangrrua.spark.pinot.datasource

import com.mangrrua.spark.pinot.BaseTest
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.JavaConverters._

class PinotDataSourceReadOptionsTest extends BaseTest {

  test("Spark DataSourceOptions should be converted to the PinotDataSourceReadOptions") {
    val options = Map(
      DataSourceOptions.TABLE_KEY -> "tbl",
      PinotDataSourceReadOptions.CONFIG_CONTROLLER -> "localhost:9000",
      PinotDataSourceReadOptions.CONFIG_BROKER -> "localhost:8000",
      PinotDataSourceReadOptions.CONFIG_SEGMENTS_PER_SPLIT -> "1",
      PinotDataSourceReadOptions.CONFIG_USE_PUSH_DOWN_FILTERS -> "false"
    )

    val datasourceOptions = new DataSourceOptions(options.asJava)
    val pinotDataSourceReadOptions = PinotDataSourceReadOptions.from(datasourceOptions)

    val expected =
      PinotDataSourceReadOptions("tbl", "localhost:9000", "localhost:8000", false, 1, 10000)

    pinotDataSourceReadOptions shouldEqual expected
  }
}

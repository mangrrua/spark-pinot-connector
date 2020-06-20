package com.mangrrua.spark.pinot.connector

import com.mangrrua.spark.pinot.BaseTest
import org.apache.spark.sql.sources._

class FilterPushDownTest extends BaseTest {

  private val supportedSparkFilters: Array[Filter] = Array(
    EqualTo("attr1", 1),
    In("attr2", Array("1", "2")),
    LessThan("attr3", 1),
    LessThanOrEqual("attr4", 3),
    GreaterThan("attr5", 10),
    GreaterThanOrEqual("attr6", 15),
    Not(EqualTo("attr7", "1")),
    And(LessThan("attr8", 10), LessThanOrEqual("attr9", 3)),
    Or(EqualTo("attr10", "hello"), GreaterThanOrEqual("attr11", 13)),
    StringContains("attr12", "pinot"),
    In("attr13", Array(10, 20))
  )

  private val unsupportedSparkFilters: Array[Filter] = Array(
    EqualNullSafe("attr13", 1),
    IsNull("attr14"),
    IsNotNull("attr15"),
    StringStartsWith("attr16", "pinot"),
    StringEndsWith("attr17", "pinot")
  )

  private val inputFilters = supportedSparkFilters ++ unsupportedSparkFilters

  test("Unsupported filters should be filtered") {
    val (accepted, postScan) = FilterPushDown.acceptFilters(inputFilters)

    accepted should contain theSameElementsAs supportedSparkFilters
    postScan should contain theSameElementsAs unsupportedSparkFilters
  }

  test("PQL query where clause should be created successfully") {
    val whereClause = FilterPushDown.compileFiltersToPqlWhereClause(supportedSparkFilters)
    val expectedOutput =
      s"(attr1 = 1) AND (attr2 IN ('1','2')) AND (attr3 < 1) AND (attr4 <= 3) AND (attr5 > 10) AND (attr6 >= 15) " +
        s"AND (NOT (attr7 = '1')) AND ((attr8 < 10) AND (attr9 <= 3)) AND ((attr10 = 'hello') OR (attr11 >= 13)) " +
        s"AND (TEXT_MATCH(attr12, 'pinot')) AND (attr13 IN (10,20))"

    whereClause.get shouldEqual expectedOutput
  }

}

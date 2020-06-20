package com.mangrrua.spark.pinot.connector.query

import com.mangrrua.spark.pinot.BaseTest
import com.mangrrua.spark.pinot.connector.TimeBoundaryInfo

class PinotQueryGeneratorTest extends BaseTest {
  private val columns = Array("c1, c2")
  private val tableName = "tbl"
  private val whereClause = Some("c1 == 5 OR c2 == 'hello'")
  private val limit = s"LIMIT ${Int.MaxValue}"

  test("Queries should be created with given filters") {
    val pinotQueries = PinotQueryGenerator.generatePQLs(tableName, None, columns, whereClause)
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE ${whereClause.get} $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE ${whereClause.get} $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Time boundary info should be added to existing where clause") {
    val timeBoundaryInfo = TimeBoundaryInfo("timeCol", "12345")
    val pinotQueries = PinotQueryGenerator
      .generatePQLs(tableName, Some(timeBoundaryInfo), columns, whereClause)

    val realtimeWhereClause = s"${whereClause.get} AND timeCol >= 12345"
    val offlineWhereClause = s"${whereClause.get} AND timeCol < 12345"
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE $realtimeWhereClause $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE $offlineWhereClause $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Time boundary info should be added to where clause") {
    val timeBoundaryInfo = TimeBoundaryInfo("timeCol", "12345")
    val pinotQueries = PinotQueryGenerator
      .generatePQLs(tableName, Some(timeBoundaryInfo), columns, None)

    val realtimeWhereClause = s"timeCol >= 12345"
    val offlineWhereClause = s"timeCol < 12345"
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE $realtimeWhereClause $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE $offlineWhereClause $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Selection query should be created with '*' column expressions without filters") {
    val pinotQueries = PinotQueryGenerator
      .generatePQLs(tableName, None, Array.empty, None)

    val expectedRealtimeQuery =
      s"SELECT * FROM ${tableName}_REALTIME $limit"
    val expectedOfflineQuery =
      s"SELECT * FROM ${tableName}_OFFLINE $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

}

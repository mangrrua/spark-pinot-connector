package com.mangrrua.spark.pinot.connector.query

import com.mangrrua.spark.pinot.connector.Constants.{PinotTableType, PinotTableTypes}
import com.mangrrua.spark.pinot.connector.{PinotUtils, TimeBoundaryInfo}

/**
  * Generate realtime and offline PQL queries for specified table with given columns and filters.
  */
private[pinot] class PinotQueryGenerator(
    tableNameWithType: String,
    timeBoundaryInfo: Option[TimeBoundaryInfo],
    columns: Array[String],
    whereClause: Option[String]) {
  private val columnsExpression = columnsAsExpression()
  private val rawTableName = PinotUtils.getRawTableName(tableNameWithType)
  private val tableType = PinotUtils.getTableType(tableNameWithType)

  def generatePQLs(): GeneratedPQLs = {
    val offlineSelectQuery = buildSelectQuery(PinotTableTypes.OFFLINE)
    val realtimeSelectQuery = buildSelectQuery(PinotTableTypes.REALTIME)
    GeneratedPQLs(
      rawTableName,
      tableType,
      offlineSelectQuery,
      realtimeSelectQuery
    )
  }

  /**
    * Get all columns if selecting columns empty(eg: resultDataFrame.count())
    */
  private def columnsAsExpression(): String = {
    if (columns.isEmpty) "*" else columns.mkString(",")
  }

  /**
    * Build realtime or offline PQL selection query.
    */
  private def buildSelectQuery(tableType: PinotTableType): String = {
    val tableNameWithType = s"${rawTableName}_$tableType"
    val queryBuilder = new StringBuilder(s"SELECT $columnsExpression FROM $tableNameWithType")

    // add where clause if exists
    whereClause.foreach { x =>
      queryBuilder.append(s" WHERE $x")
    }

    // add time boundary filter if exists
    timeBoundaryInfo.foreach { tbi =>
      val timeBoundaryFilter =
        if (tableType == PinotTableTypes.OFFLINE) {
          tbi.getOfflinePredicate
        } else {
          tbi.getRealtimePredicate
        }

      if (whereClause.isEmpty) {
        queryBuilder.append(s" WHERE $timeBoundaryFilter")
      } else {
        queryBuilder.append(s" AND $timeBoundaryFilter")
      }
    }

    // query will be converted to Pinot 'BrokerRequest' with PQL compiler
    // pinot set limit to 10 automatically
    // to prevent this add limit to query
    queryBuilder.append(s" LIMIT ${Int.MaxValue}")

    queryBuilder.toString
  }
}

private[pinot] object PinotQueryGenerator {

  def generatePQLs(
      tableNameWithType: String,
      timeBoundaryInfo: Option[TimeBoundaryInfo],
      columns: Array[String],
      whereClause: Option[String]): GeneratedPQLs = {
    new PinotQueryGenerator(tableNameWithType, timeBoundaryInfo, columns, whereClause)
      .generatePQLs()
  }
}

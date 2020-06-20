package com.mangrrua.spark.pinot.connector.query

import com.mangrrua.spark.pinot.connector.Constants.{PinotTableType, PinotTableTypes}

private[pinot] case class GeneratedPQLs(
    rawTableName: String,
    tableType: Option[PinotTableType],
    offlineSelectQuery: String,
    realtimeSelectQuery: String) {

  def isTableOffline: Boolean = {
    if (tableType.contains(PinotTableTypes.OFFLINE)) true
    else false
  }

  def isTableRealtime: Boolean = {
    if (tableType.contains(PinotTableTypes.REALTIME)) true
    else false
  }
}

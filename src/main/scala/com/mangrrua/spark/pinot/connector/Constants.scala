package com.mangrrua.spark.pinot.connector

private[pinot] object Constants {
  type PinotTableType = String

  object PinotTableTypes {
    val OFFLINE: PinotTableType = "OFFLINE"
    val REALTIME: PinotTableType = "REALTIME"
  }
}

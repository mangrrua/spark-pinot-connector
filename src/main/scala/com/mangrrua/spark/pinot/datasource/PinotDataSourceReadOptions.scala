package com.mangrrua.spark.pinot.datasource

import com.mangrrua.spark.pinot.connector.PinotClusterClient
import com.mangrrua.spark.pinot.scalafyOptional
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.util.Random

object PinotDataSourceReadOptions {
  val CONFIG_CONTROLLER = "controller"
  val CONFIG_BROKER = "broker"
  val CONFIG_USE_PUSH_DOWN_FILTERS = "usePushDownFilters"
  val CONFIG_SEGMENTS_PER_SPLIT = "segmentsPerSplit"
  val CONFIG_PINOT_SERVER_TIMEOUT_MS = "pinotServerTimeoutMs"
  private[pinot] val DEFAULT_CONTROLLER: String = "localhost:9000"
  private[pinot] val DEFAULT_USE_PUSH_DOWN_FILTERS: Boolean = true
  private[pinot] val DEFAULT_SEGMENTS_PER_SPLIT: Int = 3
  private[pinot] val DEFAULT_PINOT_SERVER_TIMEOUT_MS: Long = 10000

  private[pinot] def from(options: DataSourceOptions): PinotDataSourceReadOptions = {
    if (!options.tableName().isPresent) {
      throw new IllegalStateException(
        "Table name must be specified. eg: tbl_OFFLINE, tbl_REALTIME or tbl"
      )
    }
    val tableName = options.tableName().get()
    val controller = scalafyOptional(options.get(CONFIG_CONTROLLER)).getOrElse(DEFAULT_CONTROLLER)
    val broker = scalafyOptional(options.get(PinotDataSourceReadOptions.CONFIG_BROKER)).getOrElse {
      val brokerInstances = PinotClusterClient.getBrokerInstances(controller, tableName)
      Random.shuffle(brokerInstances).head
    }
    val usePushDownFilters =
      options.getBoolean(CONFIG_USE_PUSH_DOWN_FILTERS, DEFAULT_USE_PUSH_DOWN_FILTERS)
    val segmentsPerSplit = options.getInt(CONFIG_SEGMENTS_PER_SPLIT, DEFAULT_SEGMENTS_PER_SPLIT)
    val pinotServerTimeoutMs =
      options.getLong(CONFIG_PINOT_SERVER_TIMEOUT_MS, DEFAULT_PINOT_SERVER_TIMEOUT_MS)

    PinotDataSourceReadOptions(
      tableName,
      controller,
      broker,
      usePushDownFilters,
      segmentsPerSplit,
      pinotServerTimeoutMs
    )
  }
}

private[pinot] case class PinotDataSourceReadOptions(
    tableName: String,
    controller: String,
    broker: String,
    usePushDownFilters: Boolean,
    segmentsPerSplit: Int,
    pinotServerTimeoutMs: Long)

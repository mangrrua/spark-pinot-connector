package com.mangrrua.spark.pinot.connector

import java.util.{List => JList, Map => JMap}

import com.mangrrua.spark.pinot.connector.Constants.PinotTableTypes
import com.mangrrua.spark.pinot.datasource.PinotDataSourceReadOptions
import com.mangrrua.spark.pinot.exceptions.PinotException
import com.mangrrua.spark.pinot.utils.Logging
import com.yammer.metrics.core.MetricsRegistry
import org.apache.helix.model.InstanceConfig
import org.apache.pinot.common.metrics.BrokerMetrics
import org.apache.pinot.common.request.BrokerRequest
import org.apache.pinot.common.utils.DataTable
import org.apache.pinot.core.transport.{AsyncQueryResponse, QueryRouter, ServerInstance}
import org.apache.pinot.pql.parsers.Pql2Compiler

import scala.collection.JavaConverters._

/**
  * Fetch data from specified Pinot server.
  */
private[pinot] class PinotServerDataFetcher(
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends Logging {
  private val pqlCompiler = new Pql2Compiler()
  private val brokerId = "apache_spark"
  private val metricsRegistry = new MetricsRegistry()
  private val brokerMetrics = new BrokerMetrics(metricsRegistry)
  private val queryRouter = new QueryRouter(brokerId, brokerMetrics)

  def fetchData(): List[DataTable] = {
    val routingTableForRequest = createRoutingTableForRequest()

    val requestStartTime = System.nanoTime()
    val pinotServerAsyncQueryResponse = pinotSplit.serverAndSegments.serverType match {
      case PinotTableTypes.REALTIME =>
        val realtimeBrokerRequest =
          pqlCompiler.compileToBrokerRequest(pinotSplit.generatedPQLs.realtimeSelectQuery)
        submitRequestToPinotServer(null, null, realtimeBrokerRequest, routingTableForRequest)
      case PinotTableTypes.OFFLINE =>
        val offlineBrokerRequest =
          pqlCompiler.compileToBrokerRequest(pinotSplit.generatedPQLs.offlineSelectQuery)
        submitRequestToPinotServer(offlineBrokerRequest, routingTableForRequest, null, null)
    }

    val pinotServerResponse = pinotServerAsyncQueryResponse.getResponse.values().asScala.toList
    logInfo(s"Pinot server total response time in millis: ${System.nanoTime() - requestStartTime}")

    closePinotServerConnection()

    pinotServerResponse.foreach { response =>
      logInfo(
        s"Request stats; " +
          s"responseSize: ${response.getResponseSize}, " +
          s"responseDelayMs: ${response.getResponseDelayMs}, " +
          s"deserializationTimeMs: ${response.getDeserializationTimeMs}, " +
          s"submitDelayMs: ${response.getSubmitDelayMs}"
      )
    }

    val dataTables = pinotServerResponse
      .map(_.getDataTable)
      .filter(_ != null)

    if (dataTables.isEmpty) {
      throw PinotException(s"${pinotSplit.serverAndSegments.toString} could not respond the query")
    }

    dataTables.filter(_.getNumberOfRows > 0)
  }

  private def createRoutingTableForRequest(): JMap[ServerInstance, JList[String]] = {
    val nullZkId: String = null
    val instanceConfig = new InstanceConfig(nullZkId)
    instanceConfig.setHostName(pinotSplit.serverAndSegments.serverHost)
    instanceConfig.setPort(pinotSplit.serverAndSegments.serverPort)
    val serverInstance = new ServerInstance(instanceConfig)
    Map(
      serverInstance -> pinotSplit.serverAndSegments.segments.asJava
    ).asJava
  }

  private def submitRequestToPinotServer(
      offlineBrokerRequest: BrokerRequest,
      offlineRoutingTable: JMap[ServerInstance, JList[String]],
      realtimeBrokerRequest: BrokerRequest,
      realtimeRoutingTable: JMap[ServerInstance, JList[String]]): AsyncQueryResponse = {
    logInfo(s"Request is sending to the ${pinotSplit.serverAndSegments.toString}")
    queryRouter.submitQuery(
      partitionId,
      pinotSplit.generatedPQLs.rawTableName,
      offlineBrokerRequest,
      offlineRoutingTable,
      realtimeBrokerRequest,
      realtimeRoutingTable,
      dataSourceOptions.pinotServerTimeoutMs
    )
  }

  private def closePinotServerConnection(): Unit = {
    queryRouter.shutDown()
    logInfo("Pinot server connection closed")
  }
}

object PinotServerDataFetcher {

  def apply(
      partitionId: Int,
      pinotSplit: PinotSplit,
      dataSourceOptions: PinotDataSourceReadOptions): PinotServerDataFetcher = {
    new PinotServerDataFetcher(partitionId, pinotSplit, dataSourceOptions)
  }
}

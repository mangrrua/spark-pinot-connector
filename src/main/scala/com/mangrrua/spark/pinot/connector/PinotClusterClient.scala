package com.mangrrua.spark.pinot.connector

import java.net.{URI, URLEncoder}
import java.util.regex.Pattern

import com.mangrrua.spark.pinot.connector.Constants.PinotTableTypes
import com.mangrrua.spark.pinot.connector.query.GeneratedPQLs
import com.mangrrua.spark.pinot.decodeTo
import com.mangrrua.spark.pinot.exceptions.{HttpStatusCodeException, PinotException}
import com.mangrrua.spark.pinot.utils.{HttpUtils, Logging}
import io.circe.generic.auto._
import org.apache.pinot.spi.data.Schema

import scala.util.{Failure, Success, Try}

/**
  * Client that read/write/prepare required data from/to Pinot.
  */
private[pinot] object PinotClusterClient extends Logging {

  def getTableSchema(controllerUrl: String, tableName: String): Schema = {
    val rawTableName = PinotUtils.getRawTableName(tableName)
    Try {
      val uri = new URI(s"http://$controllerUrl/tables/$rawTableName/schema")
      val response = HttpUtils.sendGetRequest(uri)
      Schema.fromString(response)
    } match {
      case Success(response) =>
        logDebug(s"Pinot schema received successfully for table '$rawTableName'")
        response
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while getting Pinot schema for table '$rawTableName'",
          exception
        )
    }
  }

  /**
    * Get available broker urls(host:port) for given table.
    * This method is used when if broker instances not defined in the datasource options.
    */
  def getBrokerInstances(controllerUrl: String, tableName: String): List[String] = {
    val brokerPattern = Pattern.compile("Broker_(.*)_(\\d+)")
    val rawTableName = PinotUtils.getRawTableName(tableName)
    Try {
      val uri = new URI(s"http://$controllerUrl/tables/$rawTableName/instances")
      val response = HttpUtils.sendGetRequest(uri)
      val brokerUrls = decodeTo[PinotInstances](response).brokers
        .flatMap(_.instances)
        .distinct
        .map(brokerPattern.matcher)
        .filter(matcher => matcher.matches() && matcher.groupCount() == 2)
        .map { matcher =>
          val host = matcher.group(1)
          val port = matcher.group(2)
          s"$host:$port"
        }

      if (brokerUrls.isEmpty) {
        throw new IllegalStateException(s"Not found broker instance for table '$rawTableName'")
      }

      brokerUrls
    } match {
      case Success(result) =>
        logDebug(s"Broker instances received successfully for table '$tableName'")
        result
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while getting broker instances for table '$rawTableName'",
          exception
        )
    }
  }

  /**
    * Get time boundary info of specified table.
    * This method is used when table is hybrid to ensure that the overlap
    * between realtime and offline segment data is queried exactly once.
    *
    * @return time boundary info if table exist and segments push type is 'append' or None otherwise
    */
  def getTimeBoundaryInfo(brokerUrl: String, tableName: String): Option[TimeBoundaryInfo] = {
    val rawTableName = PinotUtils.getRawTableName(tableName)
    Try {
      // pinot converts the given table name to the offline table name automatically
      val uri = new URI(s"http://$brokerUrl/debug/timeBoundary/$rawTableName")
      val response = HttpUtils.sendGetRequest(uri)
      decodeTo[TimeBoundaryInfo](response)
    } match {
      case Success(decodedResponse) =>
        logDebug(s"Received time boundary for table $tableName, $decodedResponse")
        Some(decodedResponse)
      case Failure(exception) =>
        exception match {
          case e: HttpStatusCodeException if e.isStatusCodeNotFound =>
            // DO NOT THROW EXCEPTION
            // because, in hybrid table, segment push type of offline table can be 'refresh'
            // therefore, time boundary info can't found for given table
            // also if table name is incorrect, time boundary info can't found too,
            // but this method will not be called if table does not exist in pinot
            logWarning(s"Time boundary not found for table, $tableName")
            None
          case e: Exception =>
            throw PinotException(
              s"An error occurred while getting time boundary info for table '$rawTableName'",
              e
            )
        }
    }
  }

  /**
    * Fetch routing table(s) for given query(s).
    * If given table name already have type suffix, routing table found directly for given table suffix.
    * If not, offline and realtime routing tables will be got.
    *
    * Example output:
    *    - realtime ->
    *          - realtimeServer1 -> (segment1, segment2, segment3)
    *          - realtimeServer2 -> (segment4)
    *    - offline ->
    *          - offlineServer10 -> (segment10, segment20)
    *
    * @return realtime and/or offline routing table(s)
    */
  def getRoutingTable(
      brokerUrl: String,
      generatedPQLs: GeneratedPQLs): Map[String, Map[String, List[String]]] = {
    val routingTables =
      if (generatedPQLs.isTableOffline) {
        val offlineRoutingTable =
          getRoutingTableForQuery(brokerUrl, generatedPQLs.offlineSelectQuery)
        Map(PinotTableTypes.OFFLINE -> offlineRoutingTable)
      } else if (generatedPQLs.isTableRealtime) {
        val realtimeRoutingTable =
          getRoutingTableForQuery(brokerUrl, generatedPQLs.realtimeSelectQuery)
        Map(PinotTableTypes.REALTIME -> realtimeRoutingTable)
      } else {
        // hybrid table
        val offlineRoutingTable =
          getRoutingTableForQuery(brokerUrl, generatedPQLs.offlineSelectQuery)
        val realtimeRoutingTable =
          getRoutingTableForQuery(brokerUrl, generatedPQLs.realtimeSelectQuery)
        Map(
          PinotTableTypes.OFFLINE -> offlineRoutingTable,
          PinotTableTypes.REALTIME -> realtimeRoutingTable
        )
      }

    if (routingTables.values.forall(_.isEmpty)) {
      throw PinotException(s"Received routing tables are empty")
    }

    routingTables
  }

  private def getRoutingTableForQuery(brokerUrl: String, pql: String): Map[String, List[String]] = {
    Try {
      val encodedPqlQueryParam = URLEncoder.encode(pql, "UTF-8")
      val uri = new URI(s"http://$brokerUrl/debug/routingTable?pql=$encodedPqlQueryParam")
      val response = HttpUtils.sendGetRequest(uri)
      decodeTo[Map[String, List[String]]](response)
    } match {
      case Success(decodedResponse) =>
        logDebug(s"Received routing table for query $pql, $decodedResponse")
        decodedResponse
      case Failure(exception) =>
        throw PinotException(
          s"An error occurred while getting routing table for query, '$pql'",
          exception
        )
    }
  }
}

private[pinot] case class TimeBoundaryInfo(timeColumn: String, timeValue: String) {

  def getOfflinePredicate: String = s"$timeColumn < $timeValue"

  def getRealtimePredicate: String = s"$timeColumn >= $timeValue"
}

private[pinot] case class PinotInstance(tableType: String, instances: List[String])

private[pinot] case class PinotInstances(
    tableName: String,
    brokers: List[PinotInstance],
    server: List[PinotInstance])

package com.mangrrua.spark.pinot.connector

import java.util.regex.{Matcher, Pattern}

import com.mangrrua.spark.pinot.connector.Constants.PinotTableType
import com.mangrrua.spark.pinot.connector.query.GeneratedPQLs
import com.mangrrua.spark.pinot.exceptions.PinotException
import com.mangrrua.spark.pinot.utils.Logging

private[pinot] object PinotSplitter extends Logging {
  private val PINOT_SERVER_PATTERN = Pattern.compile("Server_(.*)_(\\d+)")

  def generatePinotSplits(
      generatedPQLs: GeneratedPQLs,
      routingTable: Map[String, Map[String, List[String]]],
      segmentsPerSplit: Int): List[PinotSplit] = {
    routingTable.flatMap {
      case (tableType, serversToSegments) =>
        serversToSegments
          .map { case (server, segments) => parseServerInput(server, segments) }
          .flatMap {
            case (matcher, segments) =>
              createPinotSplitsFromSubSplits(
                tableType,
                generatedPQLs,
                matcher,
                segments,
                segmentsPerSplit
              )
          }
    }.toList
  }

  private def parseServerInput(server: String, segments: List[String]): (Matcher, List[String]) = {
    val matcher = PINOT_SERVER_PATTERN.matcher(server)
    if (matcher.matches() && matcher.groupCount() == 2) matcher -> segments
    else throw PinotException(s"'$server' did not match!?")
  }

  private def createPinotSplitsFromSubSplits(
      tableType: PinotTableType,
      generatedPQLs: GeneratedPQLs,
      serverMatcher: Matcher,
      segments: List[String],
      segmentsPerSplit: Int): Iterator[PinotSplit] = {
    val serverHost = serverMatcher.group(1)
    val serverPort = serverMatcher.group(2)
    val maxSegmentCount = Math.min(segments.size, segmentsPerSplit)
    segments.grouped(maxSegmentCount).map { subSegments =>
      val serverAndSegments =
        PinotServerAndSegments(serverHost, serverPort, subSegments, tableType)
      PinotSplit(generatedPQLs, serverAndSegments)
    }
  }
}

private[pinot] case class PinotSplit(
    generatedPQLs: GeneratedPQLs,
    serverAndSegments: PinotServerAndSegments)

private[pinot] case class PinotServerAndSegments(
    serverHost: String,
    serverPort: String,
    segments: List[String],
    serverType: PinotTableType) {
  override def toString: String = s"$serverHost:$serverPort($serverType)"
}

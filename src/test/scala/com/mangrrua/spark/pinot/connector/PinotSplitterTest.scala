package com.mangrrua.spark.pinot.connector

import com.mangrrua.spark.pinot.BaseTest
import com.mangrrua.spark.pinot.connector.Constants.PinotTableTypes
import com.mangrrua.spark.pinot.connector.query.GeneratedPQLs
import com.mangrrua.spark.pinot.exceptions.PinotException

class PinotSplitterTest extends BaseTest {
  private val generatedPql = GeneratedPQLs("tbl", None, "", "")

  private val routingTable = Map(
    PinotTableTypes.OFFLINE -> Map(
      "Server_192.168.1.100_7000" -> List("segment1", "segment2", "segment3"),
      "Server_192.168.2.100_9000" -> List("segment4"),
      "Server_192.168.3.100_7000" -> List("segment5", "segment6")
    ),
    PinotTableTypes.REALTIME -> Map(
      "Server_192.168.33.100_5000" -> List("segment10", "segment11", "segment12"),
      "Server_192.168.44.100_7000" -> List("segment13")
    )
  )

  test("Total 5 partition splits should be created for maxNumSegmentPerServerRequest = 3") {
    val maxNumSegmentPerServerRequest = 3
    val splitResults =
      PinotSplitter.generatePinotSplits(generatedPql, routingTable, maxNumSegmentPerServerRequest)

    splitResults.size shouldEqual 5
  }

  test("Total 5 partition splits should be created for maxNumSegmentPerServerRequest = 90") {
    val maxNumSegmentPerServerRequest = 90
    val splitResults =
      PinotSplitter.generatePinotSplits(generatedPql, routingTable, maxNumSegmentPerServerRequest)

    splitResults.size shouldEqual 5
  }

  test("Total 10 partition splits should be created for maxNumSegmentPerServerRequest = 1") {
    val maxNumSegmentPerServerRequest = 1
    val splitResults =
      PinotSplitter.generatePinotSplits(generatedPql, routingTable, maxNumSegmentPerServerRequest)

    splitResults.size shouldEqual 10
  }

  test("Input pinot server string should be parsed successfully") {
    val inputRoutingTable = Map(
      PinotTableTypes.REALTIME -> Map("Server_192.168.1.100_9000" -> List("segment1"))
    )

    val splitResults = PinotSplitter.generatePinotSplits(generatedPql, inputRoutingTable, 5)
    val expectedOutput = List(
      PinotSplit(
        generatedPql,
        PinotServerAndSegments("192.168.1.100", "9000", List("segment1"), PinotTableTypes.REALTIME)
      )
    )

    expectedOutput should contain theSameElementsAs splitResults
  }

  test("GeneratePinotSplits method should throw exception due to wrong input Server_HOST_PORT") {
    val inputRoutingTable = Map(
      PinotTableTypes.REALTIME -> Map(
        "Server_192.168.1.100_9000" -> List("segment1"),
        "Server_192.168.2.100" -> List("segment5")
      )
    )

    val exception = intercept[PinotException] {
      PinotSplitter.generatePinotSplits(generatedPql, inputRoutingTable, 5)
    }

    exception.getMessage shouldEqual "'Server_192.168.2.100' did not match!?"
  }
}

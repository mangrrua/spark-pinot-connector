package com.mangrrua.spark.pinot.datasource

import java.util.{List => JList}

import com.mangrrua.spark.pinot.connector.query.PinotQueryGenerator
import com.mangrrua.spark.pinot.connector._
import com.mangrrua.spark.pinot.exceptions.PinotException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{
  DataSourceReader,
  InputPartition,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class PinotDataSourceReader(options: DataSourceOptions, userSchema: Option[StructType] = None)
  extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private val pinotDataSourceOptions = PinotDataSourceReadOptions.from(options)
  private var acceptedFilters: Array[Filter] = Array.empty
  private var currentSchema: StructType = _

  override def readSchema(): StructType = {
    if (currentSchema == null) {
      currentSchema = userSchema.getOrElse {
        val pinotTableSchema = PinotClusterClient.getTableSchema(
          pinotDataSourceOptions.controller,
          pinotDataSourceOptions.tableName
        )
        PinotUtils.pinotSchemaToSparkSchema(pinotTableSchema)
      }
    }
    currentSchema
  }

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
    val schema = readSchema()
    val tableType = PinotUtils.getTableType(pinotDataSourceOptions.tableName)

    // Time boundary is used when table is hybrid to ensure that the overlap
    // between realtime and offline segment data is queried exactly once
    val timeBoundaryInfo =
      if (tableType.isDefined) {
        None
      } else {
        PinotClusterClient.getTimeBoundaryInfo(
          pinotDataSourceOptions.broker,
          pinotDataSourceOptions.tableName
        )
      }

    val whereCondition = FilterPushDown.compileFiltersToPqlWhereClause(this.acceptedFilters)
    val generatedPQLs = PinotQueryGenerator.generatePQLs(
      pinotDataSourceOptions.tableName,
      timeBoundaryInfo,
      schema.fieldNames,
      whereCondition
    )

    val routingTable =
      PinotClusterClient.getRoutingTable(pinotDataSourceOptions.broker, generatedPQLs)

    PinotSplitter
      .generatePinotSplits(
        generatedPQLs,
        routingTable,
        pinotDataSourceOptions.segmentsPerSplit
      )
      .zipWithIndex
      .map {
        case (pinotSplit, partitionId) =>
          new PinotInputPartition(schema, partitionId, pinotSplit, pinotDataSourceOptions)
            .asInstanceOf[InputPartition[InternalRow]]
      }
      .asJava
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.currentSchema = requiredSchema
  }

  override def pushedFilters(): Array[Filter] = {
    this.acceptedFilters
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (pinotDataSourceOptions.usePushDownFilters) {
      val (acceptedFilters, postScanFilters) = FilterPushDown.acceptFilters(filters)
      this.acceptedFilters = acceptedFilters
      postScanFilters
    } else {
      filters
    }
  }
}

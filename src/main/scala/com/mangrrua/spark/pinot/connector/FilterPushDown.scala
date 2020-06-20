package com.mangrrua.spark.pinot.connector

import org.apache.spark.sql.sources._

private[pinot] object FilterPushDown {

  /**
    * Create PQL 'where clause' from Spark filters.
    *
    * @param filters Supported spark filters
    * @return where clause, or None if filters does not exists
    */
  def compileFiltersToPqlWhereClause(filters: Array[Filter]): Option[String] = {
    if (filters.isEmpty) {
      None
    } else {
      Option(filters.flatMap(compileFilter).map(filter => s"($filter)").mkString(" AND "))
    }
  }

  /**
    * Accept only filters that supported in Pinot Query(PQL).
    *
    * @param filters Spark filters that contains valid and/or invalid filters
    * @return Supported and unsupported filters
    */
  def acceptFilters(filters: Array[Filter]): (Array[Filter], Array[Filter]) = {
    filters.partition(isFilterSupported)
  }

  private def isFilterSupported(filter: Filter): Boolean = filter match {
    case _: EqualTo => true
    case _: In => true
    case _: LessThan => true
    case _: LessThanOrEqual => true
    case _: GreaterThan => true
    case _: GreaterThanOrEqual => true
    case _: Not => true
    case _: StringContains => true
    case _: And => true
    case _: Or => true
    case _ => false
  }

  private def compileFilter(filter: Filter): Option[String] = {
    val whereCondition = filter match {
      case EqualTo(attr, value) =>
        if (value.isInstanceOf[String]) {
          s"$attr = '$value'"
        } else {
          s"$attr = $value"
        }
      case In(attr, values) if !values.isEmpty =>
        if (values.head.isInstanceOf[String]) {
          s"""$attr IN ('${values.mkString("','")}')"""
        } else {
          s"""$attr IN (${values.mkString(",")})"""
        }
      case LessThan(attr, value) =>
        s"$attr < $value"
      case LessThanOrEqual(attr, value) =>
        s"$attr <= $value"
      case GreaterThan(attr, value) =>
        s"$attr > $value"
      case GreaterThanOrEqual(attr, value) =>
        s"$attr >= $value"
      case Not(child) =>
        compileFilter(child).map(p => s"NOT ($p)").orNull
      case StringContains(attr, value) =>
        s"TEXT_MATCH($attr, '$value')"
      case And(left, right) =>
        val andFilters = Seq(left, right).flatMap(compileFilter)
        if (andFilters.size == 2) {
          andFilters.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case Or(left, right) =>
        val orFilters = Seq(left, right).flatMap(compileFilter)
        if (orFilters.size == 2) {
          orFilters.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case _ => null
    }
    Option(whereCondition)
  }

}

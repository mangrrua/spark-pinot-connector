package com.mangrrua.spark.pinot.exceptions

private[pinot] case class HttpStatusCodeException(message: String, statusCode: Int)
  extends Exception(message) {
  def isStatusCodeNotFound: Boolean = statusCode == 404
}

private[pinot] case class PinotException(message: String, throwable: Throwable = None.orNull)
  extends Exception(message, throwable)

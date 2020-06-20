package com.mangrrua.spark.pinot.utils

import java.net.URI

import com.mangrrua.spark.pinot.exceptions.HttpStatusCodeException
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpUriRequest, RequestBuilder}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

private[pinot] object HttpUtils extends Logging {
  private val GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000 // 5 mins
  private val GET_REQUEST_CONNECT_TIMEOUT_MS = 10 * 1000 // 10 mins

  private val httpClient = HttpClients.custom().build()

  def sendGetRequest(uri: URI): String = {
    val requestConfig = RequestConfig
      .custom()
      .setConnectTimeout(GET_REQUEST_CONNECT_TIMEOUT_MS)
      .setSocketTimeout(GET_REQUEST_SOCKET_TIMEOUT_MS)
      .build()

    val requestBuilder = RequestBuilder.get(uri)
    requestBuilder.setConfig(requestConfig)
    executeRequest(requestBuilder.build())
  }

  private def executeRequest(httpRequest: HttpUriRequest): String = {
    val response = httpClient.execute(httpRequest)
    try {
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode >= 200 && statusCode < 300) {
        if (response.getEntity != null) {
          EntityUtils.toString(response.getEntity, "UTF-8")
        } else {
          throw new IllegalStateException("Http response content is empty!?")
        }
      } else {
        throw HttpStatusCodeException(
          s"Got error status code '$statusCode' with reason '${response.getStatusLine.getReasonPhrase}'",
          statusCode
        )
      }
    } finally {
      response.close()
    }
  }

  def close(): Unit = {
    httpClient.close()
  }
}

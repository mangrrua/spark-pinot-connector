package com.mangrrua.spark

import java.util.Optional

import io.circe.{Decoder, parser}

package object pinot {

  /** Parse json string to given model. */
  def decodeTo[A: Decoder](jsonString: String): A = {
    parser.decode[A](jsonString) match {
      case Left(error) =>
        throw new IllegalStateException(s"Error occurred while parsing json string, $error")
      case Right(value) =>
        value
    }
  }

  def scalafyOptional[A](value: Optional[A]): Option[A] = {
    if (value.isPresent) Some(value.get()) else None
  }
}

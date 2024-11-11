package com.khulnasoft.spark.connector.writer

case class NullKeyColumnException(columnName: String)
  extends NullPointerException(s"Invalid null value for key column $columnName")

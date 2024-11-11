package com.khulnasoft.spark.connector

import com.khulnasoft.spark.connector.writer.WriteConf

sealed trait BatchSize

case class RowsInBatch(batchSize: Int) extends BatchSize
case class BytesInBatch(batchSize: Int) extends BatchSize

object BatchSize {
  @deprecated("Use com.khulnasoft.spark.connector.FixedBatchSize instead of a number", "1.1")
  implicit def intToFixedBatchSize(batchSize: Int): RowsInBatch = RowsInBatch(batchSize)

  val Automatic = BytesInBatch(WriteConf.BatchSizeBytesParam.default)
}

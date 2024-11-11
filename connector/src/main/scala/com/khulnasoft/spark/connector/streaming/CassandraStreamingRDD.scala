package com.khulnasoft.spark.connector.streaming

import com.khulnasoft.spark.connector.cql.CassandraConnector
import com.khulnasoft.spark.connector.rdd.reader._
import com.khulnasoft.spark.connector.rdd._
import com.khulnasoft.spark.connector.{AllColumns, ColumnSelector}
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[com.khulnasoft.spark.connector.rdd.CassandraTableScanRDD]]*/
class CassandraStreamingRDD[R] private[connector] (
    sctx: StreamingContext,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    columns: ColumnSelector = AllColumns,
    where: CqlWhereClause = CqlWhereClause.empty,
    empty: Boolean = false,
    limit: Option[CassandraLimit] = None,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R],
    @transient val rrf: RowReaderFactory[R])
  extends CassandraTableScanRDD[R](
    sc = sctx.sparkContext,
    connector = connector,
    keyspaceName = keyspace,
    tableName = table,
    columnNames = columns,
    where = where,
    limit = limit,
    clusteringOrder = clusteringOrder,
    readConf = readConf)

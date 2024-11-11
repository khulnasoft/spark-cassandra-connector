package com.khulnasoft.spark.connector.util

import java.io.IOException

import com.khulnasoft.oss.driver.api.core.CqlSession
import com.khulnasoft.oss.driver.api.core.cql.PreparedStatement
import com.khulnasoft.spark.connector.cql.{ColumnDef, TableDef}
import com.khulnasoft.spark.connector.util.Quote._

import scala.collection.Seq

object PatitionKeyTools {
  /**
    * This query is only used to build a prepared statement so we can more easily extract
    * partition tokens from tables. We prepare a statement of the form SELECT * FROM keyspace.table
    * where x= .... This statement is never executed.
    */
  private[connector] def querySelectUsingOnlyPartitionKeys(tableDef: TableDef): String = {
    val partitionKeys = tableDef.partitionKey
    def quotedColumnNames(columns: Seq[ColumnDef]) = partitionKeys.map(_.columnName).map(quote)
    val whereClause = quotedColumnNames(partitionKeys).map(c => s"$c = :$c").mkString(" AND ")
    s"SELECT * FROM ${quote(tableDef.keyspaceName)}.${quote(tableDef.tableName)} WHERE $whereClause"
  }

  private[connector] def prepareDummyStatement(session: CqlSession, tableDef: TableDef): PreparedStatement = {
    try {
      session.prepare(querySelectUsingOnlyPartitionKeys(tableDef))
    }
    catch {
      case t: Throwable =>
        throw new IOException(
          s"""Failed to prepare statement
             | ${querySelectUsingOnlyPartitionKeys(tableDef)}: """.stripMargin + t.getMessage, t)
    }
  }

}

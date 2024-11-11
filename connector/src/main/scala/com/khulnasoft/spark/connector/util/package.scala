package com.khulnasoft.spark.connector

import com.khulnasoft.dse.driver.api.core.auth.ProxyAuthentication
import com.khulnasoft.oss.driver.api.core.cql.Statement
import com.khulnasoft.spark.connector.cql.{CassandraConnector, Schema, TableDef}

/** Useful stuff that didn't fit elsewhere. */
package object util {

  def maybeExecutingAs[StatementT <: Statement[StatementT]](stmt: StatementT, proxyUser: Option[String]): StatementT = {
    proxyUser match {
      case Some(user) =>
        ProxyAuthentication.executeAs(user, stmt)
      case _ =>
        stmt
    }
  }

  def schemaFromCassandra(
      connector: CassandraConnector,
      keyspaceName: Option[String] = None,
      tableName: Option[String] = None): Schema = {
    connector.withSessionDo(Schema.fromCassandra(_, keyspaceName, tableName))
  }

  def tableFromCassandra(
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String): TableDef = {
    connector.withSessionDo(Schema.tableFromCassandra(_, keyspaceName, tableName))
  }

}

package org.apache.spark.sql.cassandra

import com.khulnasoft.spark.connector.cql.TableDef
import org.apache.spark.sql.sources.Filter
import org.apache.spark.SparkConf

case class AnalyzedPredicates(
  handledByCassandra: Set[Filter],
  handledBySpark: Set[Filter] ){
  override def toString(): String = {
    s"""C* Filters: [${handledByCassandra.mkString(", ")}]
       |Spark Filters [${handledBySpark.mkString(", ")}]""".stripMargin
  }
}

trait CassandraPredicateRules{
  def apply(predicates: AnalyzedPredicates, tableDef: TableDef, conf: SparkConf): AnalyzedPredicates
}

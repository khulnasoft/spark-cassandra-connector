package com.khulnasoft.spark.connector.types

import com.khulnasoft.spark.connector.util._

case class ColumnTypeConf(customFromDriver: Option[String])

object ColumnTypeConf {

  val ReferenceSection = "Custom Cassandra Type Parameters (Expert Use Only)"

  val deprecatedCustomDriverTypeParam = DeprecatedConfigParameter(
    "spark.cassandra.dev.customFromDriver",
    None,
    deprecatedSince = "Analytics Connector 1.0",
    rational = "The ability to load new driver type converters at runtime has been removed"
  )

}
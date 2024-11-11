package com.khulnasoft.spark.connector.sql

import java.util.TimeZone

import com.khulnasoft.spark.connector.cluster.CETCluster
import org.scalatest.FlatSpec

/**
  * This should be executed in separate JVM, as Catalyst caches default time zone
  */
class CassandraDataFrameDateCETSpec  extends FlatSpec with CassandraDataFrameDateBehaviors with CETCluster {

  val centralEuropeanTimeZone = TimeZone.getTimeZone("CET")

  "A DataFrame in CET timezone" should behave like dataFrame(centralEuropeanTimeZone)
}

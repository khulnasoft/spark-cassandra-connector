package com.khulnasoft.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.khulnasoft.spark.connector.cluster.CETCluster

class DateTypeCETTest extends DateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}

class SqlDateTypeCETTest extends SqlDateTypeTest(TimeZone.getTimeZone("CET")) with CETCluster {
}

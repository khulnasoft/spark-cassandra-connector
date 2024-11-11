package com.khulnasoft.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.khulnasoft.spark.connector.cluster.{CETCluster, CSTCluster}

class DateTypeCSTTest extends DateTypeTest(TimeZone.getTimeZone("CST")) with CSTCluster {
}

class SqlDateTypeCSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("CST")) with CSTCluster {
}

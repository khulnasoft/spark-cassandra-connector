package com.khulnasoft.spark.connector.rdd.typeTests

import java.util.TimeZone

import com.khulnasoft.spark.connector.cluster.PSTCluster

class DateTypePSTTest extends DateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

class SqlDateTypePSTTest extends SqlDateTypeTest(TimeZone.getTimeZone("PST")) with PSTCluster {
}

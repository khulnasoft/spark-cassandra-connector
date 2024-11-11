package com.khulnasoft.spark.connector.types

import com.khulnasoft.dse.driver.api.core.`type`.DseDataTypes
import com.khulnasoft.dse.driver.api.core.data.time.DateRange
import com.khulnasoft.oss.driver.api.core.CqlSession
import com.khulnasoft.spark.connector.SparkCassandraITFlatSpecBase
import com.khulnasoft.spark.connector.cluster.DefaultCluster
import com.khulnasoft.spark.connector.cql.CassandraConnector
import com.khulnasoft.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._

class DateRangeTypeSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  def makeDateRangeTables(session: CqlSession): Unit = {
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.taxi_trips
         |(id int PRIMARY KEY, pickup_dropoff_range 'DateRangeType');""".stripMargin)

    session.execute(
      s"""INSERT INTO $ks.taxi_trips (id, pickup_dropoff_range)
         | VALUES (1, '[2017-02-02T14:57:00 TO 2017-02-02T15:10:17]');""".stripMargin
    )
  }

  override def beforeClass {
    dseOnly {
      conn.withSessionDo { session =>
        session.execute(
          s"""CREATE KEYSPACE IF NOT EXISTS $ks
             |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
            .stripMargin)
        makeDateRangeTables(session)
      }
    }
  }

  "The Spark Cassandra Connector" should "find a converter for DateRange types" in dseOnly {
    ColumnType.fromDriverType(DseDataTypes.DATE_RANGE) should be(DateRangeType)
  }

  it should "read DateRange types" in dseOnly {
    val result = sc.cassandraTable(ks, "taxi_trips").select("pickup_dropoff_range").collect
    val resultCC = sc.cassandraTable[(DateRange)](ks, "taxi_trips")
      .select("pickup_dropoff_range")
      .collect
    val expected = DateRange.parse("[2017-02-02T14:57:00 TO 2017-02-02T15:10:17]")
    result.head.get[DateRange](0) shouldBe expected
    resultCC.head shouldBe expected
  }

  it should "write DateRange types" in dseOnly {
    val expectedDateRange = DateRange.parse("[2018-03-02T14:57:00 TO 2018-04-02T15:10:17]")
    sc.parallelize(Seq((2, expectedDateRange))).saveToCassandra(ks, "taxi_trips")
    val result = sc.cassandraTable(ks, "taxi_trips")
      .where("id = 2")
      .select("pickup_dropoff_range")
      .collect
    result.head.get[DateRange](0) shouldBe expectedDateRange
  }

  def getDf() = {
    spark.read.cassandraFormat("taxi_trips", ks).load.select("pickup_dropoff_range")
  }

  "SparkSql" should "read DateRange types" in dseOnly {
    val row = getDf().filter(col("id") === 1).collect().head
    val expected = "[2017-02-02T14:57:00 TO 2017-02-02T15:10:17]"
    row.getString(0) shouldBe expected
  }

  it should "write DateRange types" in dseOnly {
    val expectedDateRange = "[2018-03-02T14:57:00 TO 2018-04-02T15:10:17]"
    spark.createDataFrame(Seq((3, expectedDateRange)))
      .select(col("_1") as "id", col("_2") as "pickup_dropoff_range")
      .write
      .cassandraFormat("taxi_trips", ks)
      .mode("append")
      .save()
    val row = getDf().filter(col("id") === 3).collect().head
    row.getString(0) shouldBe expectedDateRange
  }

}


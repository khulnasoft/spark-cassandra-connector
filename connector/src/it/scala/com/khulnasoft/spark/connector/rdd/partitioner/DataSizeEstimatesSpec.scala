package com.khulnasoft.spark.connector.rdd.partitioner

import com.khulnasoft.spark.connector.SparkCassandraITFlatSpecBase
import com.khulnasoft.spark.connector.cluster.DefaultCluster
import com.khulnasoft.spark.connector.cql.CassandraConnector
import com.khulnasoft.spark.connector.rdd.partitioner.dht.LongToken

import scala.language.postfixOps

class DataSizeEstimatesSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  val tableName = "table1"

  override def beforeClass: Unit = {
    conn.withSessionDo { session => createKeyspace(session) }


    conn.withSessionDo { session =>
      val executor = getExecutor(session)
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
      val ps = session.prepare(s"INSERT INTO $ks.$tableName(key, value) VALUES (?, ?)")
      awaitAll(
        for (i <- 1 to 1000) yield
          executor.executeAsync(ps.bind(i.asInstanceOf[AnyRef], "value" + i))
      )
      executor.waitForCurrentlyExecutingTasks()
    }

    cluster.refreshSizeEstimates()
  }

  "DataSizeEstimates" should "fetch data size estimates for a known table" in {
    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount should be > 500L
    estimates.partitionCount should be < 2000L
    estimates.dataSizeInBytes should be > 0L
  }

  it should "should return zeroes for an empty table" in {
    val tableName = "table2"
    conn.withSessionDo { session =>
      session.execute(s"CREATE TABLE $ks.$tableName(key int PRIMARY KEY, value VARCHAR)")
    }

    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }

  it should "return zeroes for a non-existing table" in {
    val tableName = "table3"
    val estimates = new DataSizeEstimates[Long, LongToken](conn, ks, tableName)
    estimates.partitionCount shouldBe 0L
    estimates.dataSizeInBytes shouldBe 0L
  }
}

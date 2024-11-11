package com.khulnasoft.spark.connector.rdd.typeTests

/**
 * Counters don't allow inserts and updates in the same pattern as teh other types so
 * we need to test them differently.
 */

import com.khulnasoft.oss.driver.api.core.cql.SimpleStatement
import com.khulnasoft.spark.connector._
import com.khulnasoft.spark.connector.cluster.DefaultCluster
import com.khulnasoft.spark.connector.cql.CassandraConnector

import scala.concurrent.Future

class CounterTypeTest  extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    //Create Cql3 Keypsace and Data
    counterCreateData()
  }

  val keyspaceName = "counter_ks"

  def counterCreateData() = conn.withSessionDo{ session =>
    createKeyspace(session, keyspaceName)

    val create_table = s"CREATE TABLE IF NOT EXISTS $keyspaceName.counter_norm ( key bigint PRIMARY KEY, expectedkeys counter )"
    val create_table_write = s"CREATE TABLE IF NOT EXISTS $keyspaceName.counter_write ( name text, key bigint , expectedkeys counter, PRIMARY KEY(name,key))"
    val create_table_multi_write = s"CREATE TABLE IF NOT EXISTS $keyspaceName.counter_multi_write ( name text, key bigint , data1 counter, data2 counter, data3 counter, PRIMARY KEY(name,key))"
    awaitAll(
      Future(session.execute(create_table)),
      Future(session.execute(create_table_write)),
      Future(session.execute(create_table_multi_write))
    )
    val ps = session.prepare(s"UPDATE $keyspaceName.counter_norm SET expectedkeys = expectedKeys + 1 WHERE key = ?")
    (1L to 10L).foreach { x => (1 to 10).foreach(_ => session.execute(ps.bind(x: java.lang.Long)))}
  }

  "Counters" should "be updatable via the C* Connector" in conn.withSessionDo { session =>
    val ps = session.prepare(s"SELECT * FROM $keyspaceName.counter_norm where key = ?")
    (1L to 10L).foreach( x => session.execute(ps.bind(x: java.lang.Long))
      .one()
      .getLong("expectedkeys") should equal (10))
  }

  it should "be readable via cassandraTable" in {
    var rdd = sc.cassandraTable("counter_ks","counter_norm")
    rdd.count() should equal (10)
    rdd.collect().foreach(row => row.getLong("expectedkeys") should equal(10))
  }

  it should "be writable via saveToCassandra" in {
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data1"))
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data2"))
    sc.parallelize(1L until 3L).map( x => (x.toString,x,1)).saveToCassandra("counter_ks","counter_multi_write",SomeColumns("name", "key", "data2"))
    val multiRdd = sc.cassandraTable("counter_ks", "counter_multi_write")
    multiRdd.collect().foreach(row => row.getString("data1") should equal("1"))
    multiRdd.collect().foreach(row => row.getString("data2") should equal("2"))
    multiRdd.collect().foreach(row => row.get[Option[String]]("data3") should be (None))
    multiRdd.count() should equal(2)
    sc.parallelize(0L until 10L).map( x => (x.toString,x,x)).saveToCassandra("counter_ks","counter_write")
    val rdd = sc.cassandraTable("counter_ks", "counter_write")
    rdd.collect().foreach(row => row.getLong("key") should equal(row.getLong("expectedkeys")))
    rdd.count() should equal(10)
  }

}



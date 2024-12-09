package com.khulnasoft.spark.connector.cql.sai

import com.khulnasoft.spark.connector.SparkCassandraITWordSpecBase
import com.khulnasoft.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import com.khulnasoft.spark.connector.cluster.DefaultCluster


class IndexedListSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiCollectionBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.list_test (
             |  pk_1 frozen<list<int>>,
             |  pk_2 int,
             |  list_col list<int>,
             |  frozen_list_col frozen<list<int>>,
             |  PRIMARY KEY ((pk_1, pk_2)));""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX pk_list_test_sai_idx ON $ks.list_test (full(pk_1)) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX frozen_list_test_sai_idx ON $ks.list_test (list_col) USING 'StorageAttachedIndex';""".stripMargin)

        session.execute(
          s"""CREATE CUSTOM INDEX full_list_test_sai_idx ON $ks.list_test (full(frozen_list_col)) USING 'StorageAttachedIndex';""".stripMargin)

        for (i <- (0 to 9)) {
          session.execute(s"insert into $ks.list_test " +
            s"(pk_1, pk_2, list_col, frozen_list_col) values " +
            s"([10$i, 11$i], $i, [10$i, 11$i], [10$i, 11$i])")
        }
      }
    }

  }

  // TODO: SPARKC-630
  "Index on a non-frozen list column" ignore {
    indexOnANonFrozenCollection("list_test", "list_col")
  }

  "Index on a frozen list column" should {
    indexOnAFrozenCollection("list_test", "frozen_list_col")
  }

  // Test indexed list columns
  "Index on list columns" should {
    "verify correct data is retrieved for various queries" in dseFrom(DSE_V6_8_3) {
      val df1 = df("list_test").filter(array_contains(col("list_col"), 101))
      val df2 = df("list_test").filter(array_contains(col("list_col"), 102))
      val df3 = df("list_test").filter(array_contains(col("list_col"), 103))

      val results1 = df1.collect
      val results2 = df2.collect
      val results3 = df3.collect

      results1 should have size 1
      results2 should have size 1
      results3 should have size 1

      results1.head.getList(2) should contain(101)
      results2.head.getList(2) should contain(102)
      results3.head.getList(2) should contain(103)
    }
  }
}

package com.khulnasoft.spark.connector.writer

import com.khulnasoft.dse.driver.api.core.DseProtocolVersion
import com.khulnasoft.oss.driver.api.core.{DefaultProtocolVersion, ProtocolVersion}
import com.khulnasoft.spark.connector.SparkCassandraITFlatSpecBase
import com.khulnasoft.spark.connector.cluster.DefaultCluster
import com.khulnasoft.spark.connector.cql.{CassandraConnector, Schema}
import com.khulnasoft.spark.connector.types.CassandraOption
import com.khulnasoft.spark.connector.util.schemaFromCassandra

class BoundStatementBuilderSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session, ks)
    session.execute( s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
  }

  val schema = schemaFromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, CassandraOption[String])]
    .rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  val ps = conn.withSessionDo(session =>
    session.prepare( s"""INSERT INTO "$ks".tab (id, value) VALUES (?, ?) """))

  val PVGt4 = Seq(DefaultProtocolVersion.V4, DefaultProtocolVersion.V5, DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2)
  val PVLte3 = Seq(DefaultProtocolVersion.V3)
  //TODO Switch to ```((InternalDriverContext)session.getContext()).getProtocolVersionRegistry()```

  "BoundStatementBuilder" should "ignore Unset values if ProtocolVersion >= 4" in {
    for (testProtocol <- PVGt4) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should ignore unset values :")(x.stmt.isSet("value") should be(false))
    }
  }

  it should "set Unset values to null if ProtocolVersion <= 3" in {
    for (testProtocol <- PVLte3) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should set to null :")(x.stmt.isNull("value") should be(true))
    }
  }

  it should "ignore null values if ignoreNulls is set and protocol version >= 4" in {
    val testProtocols = PVGt4
    for (testProtocol <- testProtocols) {
      val bsb = new BoundStatementBuilder(
        rowWriter,
        ps,
        protocolVersion = testProtocol,
        ignoreNulls = true)

      val x = bsb.bind((1, null))
      withClue(s"$testProtocol should ignore unset values :")(x.stmt.isSet("value") should be(false))
    }
  }

  it should "throw an exception if ignoreNulls is set and protocol version <= 3" in {
    for (testProtocol <- PVLte3) {
      intercept[IllegalArgumentException] {
        val bsb = new BoundStatementBuilder(
          rowWriter,
          ps,
          protocolVersion = testProtocol,
          ignoreNulls = true)
      }
    }
  }
}

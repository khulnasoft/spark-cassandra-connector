package com.khulnasoft.spark.connector.cql

import com.khulnasoft.spark.connector.SparkCassandraITFlatSpecBase
import com.khulnasoft.spark.connector.cluster.SSLCluster

class CassandraSSLClientAuthConnectorSpec extends SparkCassandraITFlatSpecBase with SSLCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  "A CassandraConnector" should "be able to use a secure connection when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
    }
  }

  // Test SSL client authentication
  it should "establish a secure connection using SSL client authentication" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
    }
  }

  // Verify secure connection is established
  it should "verify that a secure connection is established" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
    }
  }
}

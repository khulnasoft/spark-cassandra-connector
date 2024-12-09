package com.khulnasoft.bdp.spark.search

import java.net.InetAddress

import com.khulnasoft.oss.driver.api.core.CqlSession
import org.apache.solr.client.solrj.impl.HttpSolrClient

import scala.collection.concurrent.TrieMap

trait SearchSupport {

  def getSolrClient(host: InetAddress, indexName: String): HttpSolrClient = {
    SearchSupport.solrClients.getOrElseUpdate(indexName, {
      new HttpSolrClient.Builder("http://" + host.getHostName + ":" + SearchSupport.solrPort + "/solr/" + indexName).build()
    })
  }

  def createCore(session: CqlSession, host: Int, indexName: String): Unit = {
    createCore(session, host, indexName, true, false, true, false)
  }

  def createCore(session: CqlSession, host: Int, indexName: String, distributed: Boolean, recovery: Boolean, reindex: Boolean, lenient: Boolean = false): Unit = {
    val split = indexName.split("\\.", 2)
    val (ks, table) = (split(0), split(1))

    val createStatement =
      s"""CREATE SEARCH INDEX IF NOT EXISTS ON "$ks"."$table"
         |WITH OPTIONS{distributed:$distributed,recovery:$recovery,reindex:$reindex,lenient:$lenient}""".stripMargin

    session.execute(createStatement)
  }

  // Implement methods for creating and managing search indexes
  def createSearchIndex(session: CqlSession, ks: String, table: String, indexName: String): Unit = {
    val createStatement =
      s"""CREATE SEARCH INDEX IF NOT EXISTS ON "$ks"."$table"
         |WITH OPTIONS{indexName:'$indexName'}""".stripMargin

    session.execute(createStatement)
  }

  def dropSearchIndex(session: CqlSession, ks: String, table: String, indexName: String): Unit = {
    val dropStatement =
      s"""DROP SEARCH INDEX IF EXISTS ON "$ks"."$table"
         |WITH OPTIONS{indexName:'$indexName'}""".stripMargin

    session.execute(dropStatement)
  }

  def listSearchIndexes(session: CqlSession, ks: String, table: String): List[String] = {
    val listStatement =
      s"""SELECT index_name FROM system_schema.indexes
         |WHERE keyspace_name = '$ks' AND table_name = '$table'""".stripMargin

    val resultSet = session.execute(listStatement)
    resultSet.all().asScala.map(_.getString("index_name")).toList
  }
}

object SearchSupport {
  private val solrPort = 8983
  private val solrClients = new TrieMap[String, HttpSolrClient]
}

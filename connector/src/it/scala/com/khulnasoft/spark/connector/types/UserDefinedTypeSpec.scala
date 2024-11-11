package com.khulnasoft.spark.connector.types

import com.khulnasoft.oss.driver.api.core.CqlSession
import com.khulnasoft.spark.connector.SparkCassandraITFlatSpecBase
import com.khulnasoft.spark.connector.cluster.DefaultCluster
import com.khulnasoft.spark.connector.cql.CassandraConnector
import com.khulnasoft.spark.connector._
import org.apache.spark.sql.cassandra._

// UDTs
case class File(data: Array[Byte])

case class Profile(name: String, picture: File)

// Tables
case class Files(id: Int, file: File)

case class Profiles(id: Int, profile: Profile)

class UserDefinedTypeSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(sparkConf)

  val FilesTable = "files"
  val ProfilesTable = "profiles"

  def makeUdtTables(session: CqlSession): Unit = {
    session.execute(s"""CREATE TYPE IF NOT EXISTS $ks.file (data blob);""")
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$FilesTable
         |(id int PRIMARY KEY, file frozen<file>);""".stripMargin)

    session.execute(s"""CREATE TYPE IF NOT EXISTS $ks.profile (name text, picture frozen<file>)""")
    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $ks.$ProfilesTable
         |(id int PRIMARY KEY, profile frozen<profile>)""".stripMargin)
  }

  override def beforeClass {
    conn.withSessionDo { session =>
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $ks
           |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"""
          .stripMargin)
      makeUdtTables(session)
    }
  }

  "SparkSql" should "write UDTs with BLOB fields" in {
    val expected = File(":)".getBytes)
    spark.createDataFrame(Seq(Files(1, expected)))
      .write
      .cassandraFormat(FilesTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Files](ks, FilesTable)
      .collect()
      .head
    row.file.data shouldEqual expected.data
  }

  it should "write nested UDTs" in {
    val expected = Profile("John Smith", File(":)".getBytes))
    spark.createDataFrame(Seq(Profiles(1, expected)))
      .write
      .cassandraFormat(ProfilesTable, ks)
      .mode("append")
      .save()
    val row = spark.sparkContext
      .cassandraTable[Profiles](ks, ProfilesTable)
      .collect()
      .head
    row.profile.name shouldEqual expected.name
    row.profile.picture.data shouldEqual expected.picture.data
  }

}
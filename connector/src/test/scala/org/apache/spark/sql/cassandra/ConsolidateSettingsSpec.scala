package org.apache.spark.sql.cassandra

import com.khulnasoft.spark.connector.TableRef
import org.apache.spark.SparkConf
import com.khulnasoft.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import org.scalatest.{FlatSpec, Matchers}
import com.khulnasoft.spark.connector.rdd.ReadConf
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class ConsolidateSettingsSpec extends FlatSpec with Matchers {

  val param = ReadConf.FetchSizeInRowsParam
  val sparkConf = new SparkConf(loadDefaults = false)
  val tableRef = TableRef("table", "keyspace", Option("cluster"))
  val tableRefDefaultCluster = TableRef("table", "keyspace")

  def verify(
      sparkConf: Map[String, String],
      sqlContextConf: Map[String, String],
      tableConf: Map[String, String],
      value: Option[String],
      valueForDefaultCluster: Option[String]): Unit = {
    val sc = this.sparkConf.clone().setAll(sparkConf)

    val consolidatedConf1 = consolidateConfs(sc, sqlContextConf, tableRef.cluster.get, tableRef.keyspace, CaseInsensitiveMap(tableConf))
    val consolidatedConf2 = consolidateConfs(sc, sqlContextConf, tableRefDefaultCluster.cluster.getOrElse("default"), tableRefDefaultCluster.keyspace, CaseInsensitiveMap(Map.empty))
    consolidatedConf1.getOption(param.name) shouldBe value
    consolidatedConf2.getOption(param.name) shouldBe valueForDefaultCluster
  }

  it should "use SparkConf settings by default" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map.empty,
      tableConf = Map.empty,
      value = Some("10"),
      valueForDefaultCluster = Some("10"))
  }

  it should "override SparkConf settings by SQLContext settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(s"default/${param.name}" -> "20"),
      tableConf = Map.empty,
      value = Some("20"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override global SQLContext settings by cluster level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30"),
      tableConf = Map.empty,
      value = Some("30"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override cluster level SQLContext settings by keyspace level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30",
        s"${tableRef.cluster.get}:${tableRef.keyspace}/${param.name}" -> "40"),
      tableConf = Map.empty,
      value = Some("40"),
      valueForDefaultCluster = Some("20"))
  }

  it should "override keyspace level SQLContext settings by table level settings" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30",
        s"${tableRef.cluster.get}:${tableRef.keyspace}/${param.name}" -> "40"),
      tableConf = Map(param.name -> "50"),
      value = Some("50"),
      valueForDefaultCluster = Some("20"))
  }

  it should "be case insensitive in table conf options" in {
    verify(
      sparkConf = Map(param.name -> "10"),
      sqlContextConf = Map(
        s"default/${param.name}" -> "20",
        s"${tableRef.cluster.get}/${param.name}" -> "30",
        s"${tableRef.cluster.get}:${tableRef.keyspace}/${param.name}" -> "40"),
      tableConf = Map(param.name.toUpperCase() -> "50"),
      value = Some("50"),
      valueForDefaultCluster = Some("20"))
  }
}

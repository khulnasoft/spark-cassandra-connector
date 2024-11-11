package com.khulnasoft.spark.connector.rdd.typeTests

import java.lang.Float

import com.khulnasoft.oss.driver.api.core.cql.Row
import com.khulnasoft.spark.connector.cluster.DefaultCluster

class FloatTypeTest extends AbstractTypeTest[Float, Float] with DefaultCluster {
  override val typeName = "float"

  override val typeData: Seq[Float] = Seq(new Float(100.1), new Float(200.2),new Float(300.3), new Float(400.4), new Float(500.5))
  override val addData: Seq[Float] = Seq(new Float(600.6), new Float(700.7), new Float(800.8), new Float(900.9), new Float(1000.12))

  override def getDriverColumn(row: Row, colName: String): Float = {
    row.getFloat(colName)
  }

}


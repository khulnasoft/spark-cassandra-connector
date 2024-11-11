package com.khulnasoft.spark.connector.rdd.reader

import com.khulnasoft.oss.driver.api.core.cql.Row
import com.khulnasoft.spark.connector._
import com.khulnasoft.spark.connector.cql.TableDef
import com.khulnasoft.spark.connector.mapper._
import com.khulnasoft.spark.connector.util.JavaApiHelper

import scala.reflect.runtime.universe._


/** Transforms a Cassandra Java driver `Row` into an object of a user provided class,
  * calling the class constructor */
final class ClassBasedRowReader[R : TypeTag : ColumnMapper](
    table: TableDef,
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowReader[R] {

  private val converter =
    new GettableDataToMappedTypeConverter[R](table, selectedColumns)

  private val isReadingTuples =
    typeTag[R].tpe.typeSymbol.fullName startsWith "scala.Tuple"

  override val neededColumns = {
    val ctorRefs = converter.columnMap.constructor
    val setterRefs = converter.columnMap.setters.values
    Some(ctorRefs ++ setterRefs)
  }

  override def read(row: Row,  rowMetaData: CassandraRowMetadata): R = {
    val cassandraRow = CassandraRow.fromJavaDriverRow(row, rowMetaData)
    converter.convert(cassandraRow)
  }
}


class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {

  def columnMapper = implicitly[ColumnMapper[R]]

  override def rowReader(tableDef: TableDef, selection: IndexedSeq[ColumnRef]) =
    new ClassBasedRowReader[R](tableDef, selection)

  override def targetClass: Class[R] = JavaApiHelper.getRuntimeClass(typeTag[R])
}

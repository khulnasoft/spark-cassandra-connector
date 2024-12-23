package com.khulnasoft.spark.connector.rdd.reader

import com.khulnasoft.oss.driver.api.core.cql.Row
import com.khulnasoft.spark.connector._
import com.khulnasoft.spark.connector.cql.TableDef
import com.khulnasoft.spark.connector.types.TypeConverter
import com.khulnasoft.spark.connector.util.JavaApiHelper

class ValueRowReader[T: TypeConverter](columnRef: ColumnRef) extends RowReader[T] {

  private val converter = implicitly[TypeConverter[T]]

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    * @param row row fetched from Cassandra
    * @param rowMetaData: column names available in the `row` */
  override def read(row: Row, rowMetaData: CassandraRowMetadata): T =
    converter.convert(GettableData.get(
      row,
      columnRef.cqlValueName,
      rowMetaData.codecs(columnRef.cqlValueName)))

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  override def neededColumns: Option[Seq[ColumnRef]] =
    Some(Seq(columnRef))

}

class ValueRowReaderFactory[T: TypeConverter]
  extends RowReaderFactory[T] {

  override def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[T] = {
    require(selectedColumns.nonEmpty, "ValueRowReader requires a non-empty column selection")
    new ValueRowReader[T](selectedColumns.head)
  }

  override def targetClass: Class[T] = JavaApiHelper.getRuntimeClass(implicitly[TypeConverter[T]].targetTypeTag)
}

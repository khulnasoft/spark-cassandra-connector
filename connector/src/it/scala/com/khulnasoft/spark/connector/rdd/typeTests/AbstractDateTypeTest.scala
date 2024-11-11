package com.khulnasoft.spark.connector.rdd.typeTests

import java.sql.{Date => SqlDate}
import java.text.SimpleDateFormat
import java.util.{TimeZone, Date => UtilDate}

import com.khulnasoft.oss.driver.api.core.DefaultProtocolVersion

import scala.reflect.ClassTag
import java.time.{LocalDate}

import com.khulnasoft.oss.driver.api.core.cql.Row
import com.khulnasoft.spark.connector.rdd.reader.RowReaderFactory
import com.khulnasoft.spark.connector.types.TypeConverter
import com.khulnasoft.spark.connector.writer.RowWriterFactory

/**
  * This should be executed in separate JVM, as Catalyst caches default time zone
  */
abstract class AbstractDateTypeTest[TestType: ClassTag](
    val testTimeZone: TimeZone)(
  implicit
    tConverter: TypeConverter[TestType],
    rowReaderNormal: RowReaderFactory[(TestType, TestType, TestType, TestType)],
    rowReaderCollection: RowReaderFactory[(TestType, Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String])],
    rowReaderNull: RowReaderFactory[(TestType, TestType, Option[TestType], Set[TestType], Map[TestType, TestType], Seq[TestType])],
    rowWriterNormal: RowWriterFactory[(TestType, TestType, TestType, TestType)],
    rowWriterCollection: RowWriterFactory[(TestType, Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String])],
    rowWriterNull: RowWriterFactory[(TestType, TestType, Null, Null, Null, Null)])
  extends AbstractTypeTest[TestType, LocalDate] {

  override def minPV: DefaultProtocolVersion = DefaultProtocolVersion.V4

  TimeZone.setDefault(testTimeZone)

  private val dateRegx = """(\d\d\d\d)-(\d\d)-(\d\d).*""".r

  protected def stringToDate(str: String): TestType

  protected def dateToString(date: TestType): String

  override protected val typeName: String = "date"

  override protected lazy val typeData: Seq[TestType] = Seq(
    stringToDate("2015-05-01"),
    stringToDate("2015-05-10"),
    stringToDate("2015-05-20"),
    stringToDate("1950-03-05"))

  override protected lazy val addData: Seq[TestType] = Seq(
    stringToDate("2011-05-01"),
    stringToDate("2011-05-10"),
    stringToDate("2011-05-20"),
    stringToDate("1950-01-01"))

  override def convertToDriverInsertable(testValue: TestType): LocalDate = {
    dateToString(testValue) match {
      case dateRegx(year, month, day) =>
        LocalDate.of(year.toInt, month.toInt, day.toInt)
    }
  }

  override def getDriverColumn(row: Row, colName: String): TestType = {
    val ld = row.getLocalDate(colName)
    stringToDate(ld.toString)
  }
}

abstract class DateTypeTest(timeZone: TimeZone) extends AbstractDateTypeTest[UtilDate](timeZone) {
  private val format = new SimpleDateFormat("yyyy-MM-dd")

  override def stringToDate(str: String): UtilDate = format.parse(str)

  override def dateToString(date: UtilDate): String = format.format(date)
}

abstract class SqlDateTypeTest(timeZone: TimeZone) extends AbstractDateTypeTest[SqlDate](timeZone) {
  override def stringToDate(str: String): SqlDate = SqlDate.valueOf(str)

  override def dateToString(date: SqlDate): String = date.toString
}

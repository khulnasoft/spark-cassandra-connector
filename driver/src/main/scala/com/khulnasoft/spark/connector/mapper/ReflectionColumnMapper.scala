package com.khulnasoft.spark.connector.mapper

import java.lang.reflect.{Constructor, Method}

import com.khulnasoft.oss.driver.api.mapper.annotations.CqlName
import com.khulnasoft.spark.connector.ColumnRef
import com.khulnasoft.spark.connector.cql.StructDef
import com.khulnasoft.spark.connector.util.AnyObjectFactory._

import scala.reflect.ClassTag
import scala.util.Try

abstract class ReflectionColumnMapper[T : ClassTag] extends ColumnMapper[T] {

  protected def isSetter(method: Method): Boolean
  protected def isGetter(method: Method): Boolean

  protected def setterToColumnName(
      setterName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def getterToColumnName(
      getterName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def constructorParamToColumnName(
      paramName: String,
      columns: Map[String, ColumnRef]): Option[ColumnRef]

  protected def allowsNull: Boolean

  private def columnRefByAliasName(selectedColumns: IndexedSeq[ColumnRef]): Map[String, ColumnRef] =
    (for (c <- selectedColumns) yield (c.selectedAs, c)).toMap

  protected def annotationForFieldName(fieldName: String): Option[String] = {
    // POJO is either a table or an UDT
    // We have to cover both cases
    val maybeField = Try(cls.getField(fieldName))
    maybeField.toOption.flatMap {
      case f =>
        if (f.isAnnotationPresent(classOf[CqlName])) {
          Some(f.getAnnotation(classOf[CqlName]).value())
        } else None
    }
  }

  protected val cls = implicitly[ClassTag[T]].runtimeClass

  override def columnMapForReading(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForReading = {

    val columnByName = columnRefByAliasName(selectedColumns)

    def columnRefOrThrow(paramName: String) = {
      constructorParamToColumnName(paramName, columnByName).getOrElse {
        throw new IllegalArgumentException(
          s"Failed to map constructor parameter $paramName in $cls to a column of ${struct.name}")
      }
    }

    def columnsOf(ctor: Constructor[_]): Seq[ColumnRef] = {
      if (isNoArgsConstructor(ctor))
        Nil
      else {
        val paramNames = paranamer.lookupParameterNames(ctor)
        paramNames
          .filter(_ != "$_outer")
          .filter(!_.startsWith("this$"))
          .map(columnRefOrThrow)
      }
    }

    val constructor = columnsOf(resolveConstructor(cls))

    val setters: Map[String, ColumnRef] = {
      for {
        method <- cls.getMethods if isSetter(method)
        methodName = method.getName
        columnRef <- setterToColumnName(methodName, columnByName)
      } yield (methodName, columnRef)
    }.toMap

    new SimpleColumnMapForReading(constructor, setters, allowsNull)
  }

  override def columnMapForWriting(
      struct: StructDef,
      selectedColumns: IndexedSeq[ColumnRef]): ColumnMapForWriting = {

    val columnByName = columnRefByAliasName(selectedColumns)

    val getterMap: Map[String, ColumnRef] = {
      for {
        method <- cls.getMethods if isGetter(method)
        methodName = method.getName
        columnRef <- getterToColumnName(methodName, columnByName)
      } yield (methodName, columnRef)
    }.toMap

    // Check if we have all the required columns:
    val mappedColumns = getterMap.values.toSet
    val unmappedColumns = selectedColumns.filterNot(mappedColumns)
    require(unmappedColumns.isEmpty, s"Columns not found in $cls: [${unmappedColumns.mkString(", ")}]")

    new SimpleColumnMapForWriting(getterMap)
  }
}
